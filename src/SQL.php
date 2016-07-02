<?php

namespace Soupmix;
/*
SQL Adapter
*/

use Doctrine\DBAL\Connection;


class SQL implements Base
{
    protected $doctrine = null;
    protected $dbName = null;


    public function __construct($config, Connection $client)
    {
        $this->doctrine = $client;
        $this->dbName = $config['db_name'];
    }

    public function getConnection()
    {
        return $this->doctrine;
    }

    public function create($collection, $fields)
    {
        $schemaManager = $this->doctrine->getSchemaManager();
        $table = new SQLTable($schemaManager, $collection, $fields);
        return $table->createTable();
    }


    public function drop($collection)
    {
        $schemaManager = $this->doctrine->getSchemaManager();
        if ($schemaManager->tablesExist([$collection])) {
            return $schemaManager->dropTable($collection);
        }
        return null;
    }

    public function truncate($collection)
    {
        return $this->client->doctrine->query('TRUNCATE TABLE `' . $collection . '`');
    }

    public function createIndexes($collection, $fields)
    {

        $schemaManager = $this->doctrine->getSchemaManager();
        $table = new SQLTable($schemaManager, $collection, $fields);
        return $table->createIndexes();
    }

    public function insert($collection, $values)
    {
        $insertion = $this->doctrine->insert($collection, $values);
        if($insertion !== 0) {
            return $this->doctrine->lastInsertId();
        }
        return null;
    }

    public function update($collection, $filter, $values)
    {
        return $this->doctrine->update($collection, $values, $filter);
    }

    public function delete($collection, $filter)
    {
        $numberOfDeletedItems = $this->doctrine->delete($collection, $filter);
        if ($numberOfDeletedItems>0) {
            return 1;
        }
        return 0;
    }

    public function get($collection, $docId)
    {
        return $this->doctrine->fetchAssoc('SELECT * FROM ' . $collection . ' WHERE id = ?', array($docId));
    }

    public function find($collection, $filters, $fields = null, $sort = null, $offset = 0, $limit = 25, $debug = false)
    {
        $query = $this->query($collection);
        foreach ($filters as $filter => $value) {
            if (is_array($value)) {
                $query->orFilters($value);
            } else {
                $query->andFilter($filter, $value);
            }
        }
        return $query->returnFields($fields)
            ->sortFields($sort)
            ->offset($offset)
            ->limit($limit)
            ->run();
    }

    public function buildQuery($collection, $filters)
    {
        $queryBuilder = $this->doctrine->createQueryBuilder();
        $queryBuilder->from($collection);
        if ($filters === null) {
            return $queryBuilder;
        }
        return $this->buildQueryFilters($queryBuilder, $filters);
    }

    protected function buildQueryFilters($queryBuilder, $filters)
    {
        foreach ($filters as $key => $value) {
            if (strpos($key, '__') === false && is_array($value)) {
                $queryBuilder = $this->buildQueryForOr($queryBuilder, $value);
                continue;
            }
            $queryBuilder = $this->buildQueryForAnd($queryBuilder, $key, $value);
        }
        return $queryBuilder;
    }

    protected function buildQueryForAnd($queryBuilder, $key, $value)
    {
        $sqlOptions = self::buildFilter([$key => $value]);
        if (in_array($sqlOptions['method'], ['in', 'notIn'])) {
            $queryBuilder->andWhere(
                $queryBuilder->expr()->{$sqlOptions['method']}( $sqlOptions['key'], $sqlOptions['value'])
            );
            return $queryBuilder;
        }
        $queryBuilder->andWhere(
                '`'.$sqlOptions['key'].'`'
                . ' ' . $sqlOptions['operand']
                . ' ' . $queryBuilder->createNamedParameter($sqlOptions['value'])
            );
        return $queryBuilder;
    }
    protected function buildQueryForOr($queryBuilder, $value)
    {
        $orQuery =[];
        foreach ($value as $orValue) {
            $subKey = array_keys($orValue)[0];
            $subValue = $orValue[$subKey];
            $sqlOptions = self::buildFilter([$subKey => $subValue]);
            if (in_array($sqlOptions['method'], ['in', 'notIn'])) {
                $orQuery[] =  $queryBuilder->expr()->{$sqlOptions['method']}( $sqlOptions['key'], $sqlOptions['value']);
                continue;
            }
            $orQuery[] =
                '`'.$sqlOptions['key'].'`'
                . ' ' . $sqlOptions['operand']
                . ' ' . $queryBuilder->createNamedParameter($sqlOptions['value']);

        }
        $queryBuilder->andWhere(
            '(' . implode(' OR ', $orQuery) . ')'
        );
        return $queryBuilder;
    }

    public function query($collection)
    {
        return new SQLQueryBuilder($collection, $this);
    }

    public static function buildFilter($filter)
    {
        $key = array_keys($filter)[0];
        $value = $filter[$key];
        $operator = ' = ';
        $method = 'eq';
        $options =[
            'gte'       => ['method' => 'gte', 'operand' => ' >= '],
            'gt'        => ['method' => 'gt', 'operand' => ' > '],
            'lte'       => ['method' => 'lte', 'operand' => ' <= '],
            'lt'        => ['method' => 'lt', 'operand' => ' < '],
            'in'        => ['method' => 'in', 'operand' => ' IN '],
            '!in'       => ['method' => 'notIn', 'operand' => ' NOT IN '],
            'not'       => ['method' => 'not', 'operand' => ' NOT '],
            'wildcard'  => ['method' => 'like', 'operand' => ' LIKE '],
            'prefix'    => ['method' => 'like', 'operand' => ' LIKE '],
        ];
        if (strpos($key, '__') !== false) {
            preg_match('/__(.*?)$/i', $key, $matches);
            $key        = str_replace($matches[0], '', $key);
            $queryOperator   = $matches[1];
            $method     = $options[$queryOperator]['method'];
            $operator   = $options[$queryOperator]['operand'];
            switch ($queryOperator) {
                case 'wildcard':
                    $value = '%'.str_replace(array('?', '*'), array('_', '%'), $value).'%';
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
