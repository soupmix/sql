<?php

namespace Soupmix;
/*
SQL Adapter
*/

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Schema\Index;

class SQL implements Base
{
    public $conn = null;
    private $defaults = [
        'db_name'   => 'default',
        'user_name' => '',
        'password'  => '',
        'host'      => '127.0.0.1',
        'port'      => 3306,
        'charset'   => 'utf8',
        'driver'    => 'pdo_mysql',
    ];
    private static $columnDefaults = [
        'name'      => null,
        'type'      => 'string',
        'type_info' => null,
        'maxLength' => 255,
        'default'   => null,
        'index'     => null,
        'index_type'=> null,
]   ;

    public function __construct($config)
    {
        $config = array_merge($this->defaults, $config);
        $this->connect($config);
    }

    public function connect($config)
    {
        $connectionParams = array(
            'dbname' => $config['db_name'],
            'user' => $config['user_name'],
            'password' => $config['password'],
            'host' => $config['host'],
            'port' => $config['port'],
            'charset' => $config['charset'],
            'driver' => $config['driver'],
        );
        $this->conn = DriverManager::getConnection($connectionParams);
    }

    public function create($collection, $fields)
    {
        $columns = [];
        $indexes = [];
        $schemaManager = $this->conn->getSchemaManager();
        $columns[] = new Column('id', Type::getType('integer'), ['unsigned' => true, 'autoincrement' => true] );
        $indexes[] = new Index($collection.'_PK', ['id'], false, true);
        $tmpIndexes = [];
        foreach ($fields as $field){
            $field = array_merge(self::$columnDefaults, $field);
            $options = [];
            if ($field['type'] == 'integer' && $field['type_info'] == 'unsigned') {
                $options['unsigned'] = true;
            }
            $options['length'] = $field['maxLength'];
            $options['default'] = $field['default'];
            if ($field['index'] !== null) {
                if ( $field['index_type'] == 'unique' ) {
                    $indexes[] = new Index($collection . '_' . $field['name'] . '_UNQ', [$field['name']], true, false);
                } else {
                    $tmpIndexes[] = $field['name'];
                }
            }

            $columns[] = new Column($field['name'], Type::getType($field['type']), $options );
        }
        if(count($tmpIndexes)>0){
            $indexes[] = new Index($collection . '_IDX', $tmpIndexes, false, false);
        }
        $table = new Table($collection, $columns, $indexes);
        return $schemaManager->createTable($table);
    }

    public function drop($collection)
    {
        $schemaManager = $this->conn->getSchemaManager();
        if ($schemaManager->tablesExist([$collection])) {
            return $schemaManager->dropTable($collection);
        } else {
            return null;
        }
    }

    public function truncate($collection)
    {
        return $this->client->conn->query('TRUNCATE TABLE `' . $collection . '`');
    }

    public function createIndexes($collection, $indexes)
    {
        $schemaManager = $this->conn->getSchemaManager();

        $tmpIndexes = [];
        foreach ($indexes as $field){
            $field = array_merge(self::$columnDefaults, $field);
            if ($field['index'] !== null) {
                if ( $field['index_type'] == 'unique' ) {
                    $indexes[] = new Index($collection . '_' . $field['name'] . '_UNQ', [$field['name']], true, false);
                } else {
                    $tmpIndexes[] = $field['name'];
                }
            }
        }
        if (count($tmpIndexes) > 0) {
            $indexes[] = new Index($collection . '_IDX', $tmpIndexes, false, false);
        }
        foreach ($indexes as $index) {
            $schemaManager->createIndex($index, $collection);
        }


    }

    public function insert($collection, $values)
    {
        $insertion = $this->conn->insert($collection, $values);
        if($insertion !== 0) {
            return $this->conn->lastInsertId();
        }
        return null;
    }

    public function update($collection, $filter, $values)
    {
        return $this->conn->update($collection, $values, $filter);
    }

    public function delete($collection, $filter)
    {
        $numberOfDeletedItems = $this->conn->delete($collection, $filter);
        if ($numberOfDeletedItems>0) {
            return 1;
        }
        return 0;
    }

    public function get($collection, $docId)
    {
        return $this->conn->fetchAssoc('SELECT * FROM '.$collection.' WHERE id = ?', array($docId));
    }

    public function find($collection, $filters, $fields = null, $sort = null, $start = 0, $limit = 25, $debug = false)
    {
        $result = null;
        $queryBuilder = $this->conn->createQueryBuilder();
        $queryBuilder->from($collection);
        if ($filters !== null) {
            foreach ($filters as $key => $value) {
                 if (strpos($key, '__') === false && is_array($value)) {
                    foreach ($value as $orValue) {
                        $subKey = array_keys($orValue)[0];
                        $subValue = $orValue[$subKey];
                        $sqlOptions = self::buildFilter([$subKey=>$subValue]);
                        if(in_array($sqlOptions['method'], ['in','notIn',''])){
                            $queryBuilder->orWhere(
                                $queryBuilder->expr()->{$sqlOptions['method']}( $sqlOptions['key'], $sqlOptions['value'])
                            );
                        }
                        else{
                            $queryBuilder->orWhere(
                                $sqlOptions['key']
                                . ' ' . $sqlOptions['operand']
                                . ' ' . $queryBuilder->createNamedParameter($sqlOptions['value']));
                        }
                    }
                } else {
                    $sqlOptions = self::buildFilter([$key=>$value]);
                    if(in_array($sqlOptions['method'], ['in', 'notIn', ''])){
                        $queryBuilder->andWhere(
                            $queryBuilder->expr()->{$sqlOptions['method']}( $sqlOptions['key'], $sqlOptions['value'])
                        );
                    }
                    else{
                        $queryBuilder->andWhere(
                            $sqlOptions['key']
                            . ' ' . $sqlOptions['operand']
                            . ' ' . $queryBuilder->createNamedParameter($sqlOptions['value']));
                    }
                }
            }
        }
        if ($sort !== null) {
            $params['sort'] = '';
            foreach ($sort as $sort_key => $sort_dir) {
                if ($params['sort']!='') {
                    $params['sort'] .= ',';
                }
                $queryBuilder->addOrderBy($sort_key, $sort_dir);
            }
        }
        $queryBuilderCount = clone $queryBuilder;
        $queryBuilderCount->select(" COUNT(*) AS total ");
        $stmt = $this->conn->executeQuery($queryBuilderCount->getSql(), $queryBuilderCount->getParameters());
        $count = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $numberOfSet = 0;
        if (isset($count[0]['total']) && ($count[0]['total']>0)) {
            $numberOfSet = $count[0]['total'];
            $fields = ($fields === null) ? "*" : $fields;
            $queryBuilder->select($fields)
                ->setFirstResult($start)
                ->setMaxResults($limit);
            $stmt = $this->conn->executeQuery($queryBuilder->getSql(), $queryBuilder->getParameters());
            $result = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        }
        return ['total' => $numberOfSet, 'data' => $result];
    }

    public function query($query)
    {
        // reserved
    }

    public static function buildFilter($filter)
    {
        $key = array_keys($filter)[0];
        $value = $filter[$key];
        $operator = ' = ';
        $method = 'eq';

        $methods = [
            'gte'   => 'gte',
            'gt'    => 'gt',
            'lte'   => 'lte',
            'lt'    => 'lt',
            'in'    => 'in',
            '!in'   => 'notIn',
            'not'   => 'not',
            'wildchard' => 'like',
            'prefix' => 'like',
        ];
        $operands = [
            'gte'   => ' >= ',
            'gt'    => ' > ',
            'lte'   => ' <= ',
            'lt'    => ' < ',
            'in'    => ' IN ',
            '!in'   => ' NOT IN',
            'not'   => ' NOT',
            'wildchard' => ' LIKE ',
            'prefix' => ' LIKE ',
        ];

        if (strpos($key, '__')!==false) {
            preg_match('/__(.*?)$/i', $key, $matches);
            $key        = str_replace($matches[0], '', $key);
            $operator   = $matches[1];
            $method     = $methods[$operator];
            $operator   = $operands[$operator];
            switch ($operator) {
                case 'wildcard':
                    $value = str_replace(array('?', '*'), array('_', '%'), $value);
                    break;
                case 'prefix':
                    $value = $value.'%';
                    break;
            }
        }
        return [
            'key'       => $key,
            'operand'   => $operator,
            'method'    => $method,
            'value'     => $value
        ];

    }


}
