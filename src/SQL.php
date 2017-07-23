<?php

namespace Soupmix;

/*
SQL Adapter
*/

use Doctrine\DBAL\Connection;

final class SQL implements Base
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

    public function create(string $collection, array $fields)
    {
        $schemaManager = $this->doctrine->getSchemaManager();
        $table = new SQLTable($schemaManager, $collection, $fields);
        return $table->createTable();
    }

    public function drop(string $collection)
    {
        $schemaManager = $this->doctrine->getSchemaManager();
        if ($schemaManager->tablesExist([$collection])) {
            return $schemaManager->dropTable($collection);
        }
        return null;
    }

    public function truncate(string $collection)
    {
        return $this->client->doctrine->query('TRUNCATE TABLE ' . $collection . '');
    }

    public function createIndexes(string $collection, array $fields)
    {
        $schemaManager = $this->doctrine->getSchemaManager();
        $table = new SQLTable($schemaManager, $collection, $fields);
        return $table->createOnlyIndexes();
    }

    public function insert(string $collection, array $values)
    {
        $insertion = $this->doctrine->insert($collection, $values);
        if ($insertion !== 0) {
            return $this->doctrine->lastInsertId();
        }
        return null;
    }

    public function update(string $collection, array $filter, array $values)
    {
        return $this->doctrine->update($collection, $values, $filter);
    }

    public function delete(string $collection, array $filter)
    {
        $numberOfDeletedItems = $this->doctrine->delete($collection, $filter);
        return ($numberOfDeletedItems > 0) ? 1 : 0;
    }

    public function get(string $collection, $docId)
    {
        return $this->doctrine->fetchAssoc('SELECT * FROM ' . $collection . ' WHERE id = ?', array($docId));
    }

    public function find(string $collection, ?array $filters, ?array $fields = null, ?array $sort = null, ?int $offset = 0, ?int $limit = 25)
    {
        $query = $this->query($collection);
        foreach ($filters as $filter => $value) {
            if (is_array($value)) {
                $query->orFilters($value);
                continue;
            }
            $query->andFilter($filter, $value);
        }
        return $query->returnFields($fields)
            ->sortFields($sort)
            ->offset($offset)
            ->limit($limit)
            ->run();
    }

    public function query(string $collection)
    {
        return new SQLQueryBuilder($collection, $this);
    }
}
