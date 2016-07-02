<?php

namespace Soupmix;


class SQLQueryBuilder extends AbstractQueryBuilder
{

    private $queryBuilder = null;

    public function run(){
        $this->queryBuilder = $this->getQueryBuilder();
        $count = $this->getCount();
        if (!isset($count[0]['total']) || ($count[0]['total']==0)) {
            return ['total' => 0, 'data' => null];
        }
        $numberOfRows = $count[0]['total'];
        $this->setSortOrders();
        $this->setReturnFields();
        $this->setOffsetAndLimit();
        $stmt = $this->soupmix->getConnection()->executeQuery(
            $this->queryBuilder->getSql(),
            $this->queryBuilder->getParameters()
        );
        $result = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        if($this->distinctFieldName !== null){
                $numberOfRows = count($result);
        }
        return ['total' => $numberOfRows, 'data' => $result];
    }

    private function getQueryBuilder()
    {
        if ($this->orFilters !== null){
            $this->andFilters[] = $this->orFilters;
        }
        $this->filters      = $this->andFilters;
        return $this->buildQuery($this->collection, $this->filters);
    }

    private function getCount()
    {
        $queryBuilderCount = clone $this->queryBuilder;
        $queryBuilderCount->select(" COUNT(*) AS total ");
        $stmt = $this->soupmix->getConnection()->executeQuery($queryBuilderCount->getSql(), $queryBuilderCount->getParameters());
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    private function setSortOrders()
    {
        if ($this->sortFields !== null) {
            foreach ($this->sortFields as $sortKey => $sortDir) {
                $this->queryBuilder->addOrderBy($sortKey, $sortDir);
            }
        }
    }

    private function setReturnFields()
    {
        if ($this->distinctFieldName === null) {
            $fieldNames = ($this->fieldNames === null) ? "*" : $this->fieldNames;
            $this->queryBuilder->select($fieldNames);
            return;
        }
        $this->queryBuilder->select('DISTINCT (`' . $this->distinctFieldName . '`)');
    }

    private function setOffsetAndLimit()
    {
        $this->queryBuilder->setFirstResult($this->offset)
            ->setMaxResults($this->limit);
    }


    protected function buildQuery($collection, $filters)
    {
        $queryBuilder = $this->conn->createQueryBuilder();
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