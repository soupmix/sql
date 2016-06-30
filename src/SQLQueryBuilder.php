<?php

namespace Soupmix;


class SQLQueryBuilder extends AbstractQueryBuilder
{

    public function run(){
        $this->andFilters[]= $this->orFilters;
        $this->filters    = $this->andFilters;
        $queryBuilder = $this->soupmix->buildQuery($this->collection, $this->filters);
        $queryBuilderCount = clone $queryBuilder;
        $queryBuilderCount->select(" COUNT(*) AS total ");
        $stmt = $this->soupmix->getConnection()->executeQuery($queryBuilderCount->getSql(), $queryBuilderCount->getParameters());
        $count = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $numberOfSet = 0;
        if (isset($count[0]['total']) && ($count[0]['total']>0)) {
            $numberOfSet = $count[0]['total'];
            if ($this->sortFields !== null) {
                $params['sort'] = '';
                foreach ($this->sortFields as $sort_key => $sort_dir) {
                    if ($params['sort']!='') {
                        $params['sort'] .= ',';
                    }
                    $queryBuilder->addOrderBy($sort_key, $sort_dir);
                }
            }
            if ($this->distinctFieldName === null) {
                $fieldNames = ($this->fieldNames === null) ? "*" : $this->fieldNames;
                $queryBuilder->select($fieldNames)
                    ->setFirstResult($this->offset)
                    ->setMaxResults($this->limit);
            }
            else {
                $queryBuilder->select('DISTINCT (`' . $this->distinctFieldName . '`)');
            }
            $stmt = $this->soupmix->getConnection()->executeQuery($queryBuilder->getSql(), $queryBuilder->getParameters());
            $result = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            if($this->distinctFieldName !== null){
                $numberOfSet = count($result);
            }
        }
        return ['total' => $numberOfSet, 'data' => $result];
    }
}