<?php


namespace Soupmix;

use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Schema\Index;

class SQLTable
{
    protected $schemaManager = null;
    protected $collection = null;
    protected $fields = null;
    protected $columns = [];
    protected $indexes = [];
    protected $tmpIndexes = [];
    protected static $columnDefaults = [
        'name'      => null,
        'type'      => 'string',
        'type_info' => null,
        'maxLength' => 255,
        'default'   => null,
        'index'     => null,
        'index_type'=> null,
    ];

    public function __construct($schemaManager, $collection, $fields)
    {
        $this->schemaManager = $schemaManager;
        $this->collection = $collection;
        $this->fields = $fields;
    }

    public function createTable()
    {
        $this->buildColumns();
        $this->buildIndexes();
        $table = new Table($this->collection, $this->columns, $this->indexes);
        return $this->schemaManager->createTable($table);
    }

    protected function buildColumns()
    {
        $this->columns[] = new Column('id', Type::getType('integer'), ['unsigned' => true, 'autoincrement' => true] );
        foreach ($this->fields as $field){
            $field = array_merge(self::$columnDefaults, $field);
            $options = [];
            if ($field['type'] == 'integer' && $field['type_info'] == 'unsigned') {
                $options['unsigned'] = true;
            }
            $options['length'] = $field['maxLength'];
            $options['default'] = $field['default'];        
            $this->columns[] = new Column($field['name'], Type::getType($field['type']), $options );
        }
    }

    protected function buildIndexes()
    {
        $this->indexes[] = new Index($this->collection.'_PK', ['id'], false, true);
        foreach ($this->fields as $field){
            $field = array_merge(self::$columnDefaults, $field);
            if ($field['index'] !== null) {
                if ( $field['index_type'] == 'unique' ) {
                    $this->indexes[] = new Index($this->collection . '_' . $field['name'] . '_UNQ', [$field['name']], true, false);
                    continue;
                }
                $this->tmpIndexes[] = $field['name'];
            }
        }
        if(count($this->tmpIndexes)>0){
            $this->indexes[] = new Index($this->collection . '_IDX', $this->tmpIndexes, false, false);
        }
    }

    public function createOnlyIndexes()
    {
        $this->createIndexes();
        foreach ($this->indexes as $index) {
            $this->schemaManager->createIndex($index, $this->collection);
        }
        return true;
    }
}