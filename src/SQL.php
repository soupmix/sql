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
                if (strpos($key, '__')!==false) {
                    $sqlOptions = self::buildFilter([$key=>$value]);
                    $queryBuilder->andWhere(
                        $sqlOptions['key']
                        . ' ' . $sqlOptions['operand']
                        . ' ' . $queryBuilder->createNamedParameter($sqlOptions['value']));
                } elseif (strpos($key, '__') === false && is_array($value)) {
                    foreach ($value as $orValue) {
                        $subKey = array_keys($orValue)[0];
                        $subValue = $orValue[$subKey];
                        if (strpos($subKey, '__')!==false) {
                            $sqlOptions = self::buildFilter([$subKey=>$subValue]);
                            $queryBuilder->orWhere(
                                $sqlOptions['key']
                                . ' ' . $sqlOptions['operand']
                                . ' ' . $queryBuilder->createNamedParameter($sqlOptions['value']));
                        } else {
                            $queryBuilder->orWhere($subKey . '=' . $queryBuilder->createNamedParameter($subValue));
                        }
                    }
                } else {
                    $queryBuilder->andWhere($key . "=" . $queryBuilder->createNamedParameter($value));
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
        $queryBuilderForResult = clone $queryBuilder;
        $queryBuilder->select(" COUNT(*) AS total ");
        $stmt = $this->conn->executeQuery($queryBuilder->getSql(), $queryBuilder->getParameters());
        $count = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $numberOfSet = 0;
        if (isset($count[0]['total']) && ($count[0]['total']>0)) {
            $numberOfSet = $count[0]['total'];
            $fields = ($fields === null) ? "*" : $fields;
            $queryBuilderForResult->select($fields)
                ->setFirstResult($start)
                ->setMaxResults($limit);
            $stmt = $this->conn->executeQuery($queryBuilderForResult->getSql(), $queryBuilderForResult->getParameters());
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
        if (strpos($key, '__')!==false) {
            preg_match('/__(.*?)$/i', $key, $matches);
            $operator = $matches[1];
            switch ($operator) {
                case 'gte':
                    $operator = ' >= ';
                    break;
                case 'gt':
                    $operator = ' > ';
                    break;
                case 'lte':
                    $operator = ' <= ';
                    break;
                case 'lt':
                    $operator = ' < ';
                    break;
                case 'in':
                    $operator = ' IN ';
                    break;
                case '!in':
                    $operator = ' NOT IN ';
                    break;
                case 'not':
                    $operator = ' != ';
                    break;
                case 'wildcard':
                    $operator = ' LIKE ';
                    $value = str_replace(array('?','*'), array('_','%'), $value);
                    break;
                case 'prefix':
                    $operator = ' LIKE ';
                    $value = $value.'%';
                    break;
            }
        }
        return [
            'key'       => str_replace($matches[0], '', $key),
            'operand'   => $operator,
            'value'     => $value
        ];

    }


}
