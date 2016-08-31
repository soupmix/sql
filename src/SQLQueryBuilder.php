<?php

namespace Soupmix;


class SQLQueryBuilder extends AbstractQueryBuilder
{

    private $queryBuilder = null;

    public function run() {
        $this->queryBuilder = $this->getQueryBuilder();
        $this->setJoins();
        $count = $this->getCount();
        if (!isset($count[0]['total']) || ($count[0]['total']==0)) {
            return ['total' => 0, 'data' => null];
        }
        $numberOfRows = $count[0]['total'];
        $this->setSortOrders();
        $this->setOffsetAndLimit();
        $this->setReturnFields();
        $stmt = $this->conn->executeQuery(
            $this->queryBuilder->getSql(),
            $this->queryBuilder->getParameters()
        );
        $result = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        if ($this->distinctFieldName !== null) {
                $numberOfRows = count($result);
        }
        return ['total' => $numberOfRows, 'data' => $result];
    }

    private function getQueryBuilder()
    {
        if ($this->orFilters !== null) {
            $this->andFilters[] = $this->orFilters;
        }
        $this->filters      = $this->andFilters;
        return $this->buildQuery($this->collection, $this->filters);
    }

    private function getCount()
    {
        $queryBuilderCount = clone $this->queryBuilder;
        $queryBuilderCount->select(" COUNT(*) AS total ");
        $stmt = $this->conn->executeQuery($queryBuilderCount->getSql(), $queryBuilderCount->getParameters());
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    private function setSortOrders()
    {
        if ($this->sortFields !== null) {
            foreach ($this->addAlias($this->sortFields) as $sortKey => $sortDir) {
                $this->queryBuilder->addOrderBy($sortKey, $sortDir);
            }
        }
    }

    private function setJoins()
    {
        $this->setJoinsForType('innerJoin');
        $this->setJoinsForType('leftJoin');
        $this->setJoinsForType('rightJoin');
        $this->setJoinsForType('outerJoin');
    }

    private function setJoinsForType($joinType)
    {
        if (is_null($this->{$joinType})) {
           return;
        }
        foreach ($this->{$joinType} as $collectionName => $collection) {
            $fieldNames = $this->addAlias($collection['returnFields'], $collectionName);
            $this->returnFieldsForJoin($fieldNames);
            $joinCondition = '';
            foreach ($collection['relations'] as $relation) {
                $joinCondition .= ($joinCondition=='') ? '':' AND ';
                $relationType = array_keys($relation)[0];
                $source = array_keys($relation[$relationType])[0];
                $condition = $collectionName . "." . $source
                    . " = " . $this->collection . "." . $relation[$relationType][$source];
                if($relationType != 'field') {
                    $condition = $collectionName . "." . $source . " "
                        . $relationType . " " . $relation[$relationType][$source];
                }
                $joinCondition .= $condition;
            }
            $this->queryBuilder->$joinType($this->collection, $collectionName, $collectionName, $joinCondition);

        }
        return $this;
    }
    public function returnFieldsForJoin(array $fieldNames=null)
    {
        if($fieldNames !== null ) {
            foreach ($fieldNames as $fieldName) {
                $this->fieldNames[] = $fieldName;
            }
        }
    }

    private function setReturnFields()
    {
        if ($this->distinctFieldName === null) {
            $fieldNames = ($this->fieldNames === null) ? $this->addAlias("*") : $this->addAlias($this->fieldNames);
            $this->queryBuilder->select($fieldNames);
            return;
        }
        $this->queryBuilder->select('DISTINCT (`' . $this->collection . "`.`" . $this->distinctFieldName . '`)');
    }

    private function setOffsetAndLimit()
    {
        $this->queryBuilder->setFirstResult($this->offset)
            ->setMaxResults($this->limit);
    }

    private function addAlias($fields, $collection=null)
    {
        $collection = (!is_null($collection)) ? $collection : $this->collection;
        if (!is_array($fields)) {
            return  $collection . "." . $fields;
        }
        if (!is_array($fields)) {
           return  $collection . "." . $fields;
        }
        $newFields = [];
        foreach ($fields as $field => $value) {
            if (strpos($value, ".")!== false) {
                $newFields[] = $value;
                continue;
            }
            if (is_int($field)) {
                if (!is_array($fields)) {
                    $newFields[] = $collection . "." . $fields;
                    continue;
                }
                $newFields[] = $collection . "." . $value;
                continue;
            }
            $newFields[$collection.".".$field] = $value;
        }
        return $newFields;
    }

    protected function buildQuery($collection, $filters)
    {
        $queryBuilder = $this->conn->createQueryBuilder();
        $queryBuilder->from($collection, $collection);
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
                $queryBuilder->expr()->{$sqlOptions['method']}($this->collection . "." . $sqlOptions['key'], $sqlOptions['value'])
            );
            return $queryBuilder;
        }
        $queryBuilder->andWhere(
            '`' . $this->collection . "`.`" . $sqlOptions['key'].'`'
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
                $orQuery[] =  $queryBuilder->expr()->{$sqlOptions['method']}($this->collection . "." . $sqlOptions['key'], $sqlOptions['value']);
                continue;
            }
            $orQuery[] =
                '`' . $this->collection . "`.`" . $sqlOptions['key'].'`'
                . ' ' . $sqlOptions['operand']
                . ' ' . $queryBuilder->createNamedParameter($sqlOptions['value']);
        }
        $queryBuilder->andWhere(
            '(' . implode(' OR ', $orQuery) . ')'
        );
        return $queryBuilder;
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
            'not'       => ['method' => 'not', 'operand' => ' != '],
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