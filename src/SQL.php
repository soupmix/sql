<?php

namespace Soupmix;
/*
SQL Adapter
*/

use Doctrine\DBAL\DriverManager;

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

    public function create($collection)
    {
    }

    public function drop($collection)
    {

    }

    public function truncate($collection)
    {
    }

    public function createIndexes($collection, $indexes)
    {
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
