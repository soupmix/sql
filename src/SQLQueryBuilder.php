<?php

namespace Soupmix;


class SQLQueryBuilder extends AbstractQueryBuilder
{

    private $queryBuilder;

    public function run(){
        $this->queryBuilder = $this->getQueryBuilder();
        $count = $this->getCount();
        $numberOfRows = 0;
        if (isset($count[0]['total']) && ($count[0]['total']>0)) {
            $numberOfRows = $count[0]['total'];
            $this->setSortOrders();
            $this->setReturnFields();
            $this->setOffsetAndLimit();
            $stmt = $this->soupmix->getConnection()
                ->executeQuery(
                    $this->queryBuilder->getSql(),
                    $this->queryBuilder->getParameters()
                );
            $result = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            if($this->distinctFieldName !== null){
                $numberOfRows = count($result);
            }
        }
        return ['total' => $numberOfRows, 'data' => $result];
    }

    private function getQueryBuilder()
    {
        $this->andFilters[] = $this->orFilters;
        $this->filters      = $this->andFilters;
        return $this->soupmix->buildQuery($this->collection, $this->filters);
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
            foreach ($this->sortFields as $sort_key => $sort_dir) {
                $this->queryBuilder->addOrderBy($sort_key, $sort_dir);
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
}