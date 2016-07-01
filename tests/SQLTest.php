<?php
namespace tests;

use Soupmix\SQL;
use Doctrine\DBAL\DriverManager;
class SQLTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var \Soupmix\SQL $client
     */
    protected $client = null;

    protected function setUp()
    {
        ini_set("date.timezone", "Europe/Istanbul");

        $config = [
            'dbname'    => 'test',
            'user'      => 'root',
            'password'  => '',
            'host'      => '127.0.0.1',
            'port'      => 3306,
            'charset'   => 'utf8',
            'driver'    => 'pdo_mysql',
        ];

        $client = DriverManager::getConnection($config);

        $this->client = new SQL(['db_name'=>$config['dbname']], $client);
        $fields = [
            ['name' => 'title','type' => 'string', 'index' => true, 'index_type' => 'unique'],
            ['name' => 'age','type' =>'smallint', 'maxLength' => 3, 'default' => 24, 'index' => true],
            ['name' => 'count','type'=>'smallint', 'maxLength' => 3, 'default' => 0, 'index' => true ],
            ['name' => 'balance','type'=>'float', 'maxLength' => 3, 'default' => 0.0],
            ['name' => 'date','type'=>'datetime']
        ];
        $fields = [
            ['name' => 'title','type' => 'string', 'index' => true, 'index_type' => 'unique'],
            ['name' => 'age','type' =>'smallint', 'maxLength' => 3, 'default' => 24, 'index' => true],
            ['name' => 'count','type'=>'smallint', 'maxLength' => 3, 'default' => 0, 'index' => true ],
            ['name' => 'balance','type'=>'float', 'maxLength' => 3, 'default' => 0.0],
            ['name' => 'date','type'=>'datetime']
        ];
        $this->client->drop('test');
        $this->client->create('test', $fields);
    }


    public function testInsertGetDocument()
    {
        $docId = $this->client->insert('test', ['id' => 1, 'title' => 'test','date' => date("Y-m-d H:i:s")]);
        $document = $this->client->get('test', $docId);
        $this->assertArrayHasKey('title', $document);
        $this->assertArrayHasKey('id', $document);
        $result = $this->client->delete('test', ['id' => $docId]);
        $this->assertTrue($result == 1);
    }


    public function testFindDocuments()
    {
        $docIds = [];
        $data = $this->bulkData();
        foreach ($data as $d) {
            $docId = $this->client->insert('test', $d);
            $this->assertNotNull($docId, 'Document could not inserted to SQL while testing find');
            if ($docId) {
                $docIds[] = $docId;
            }
        }
        $results = $this->client->find('test', ['title' => 'test1']);
        $this->assertArrayHasKey('total', $results);
        $this->assertArrayHasKey('data', $results);
        $this->assertArrayHasKey('title', $results['data'][0]);

        $results = $this->client->find('test', ['count__gte' => 6]);
        $this->assertGreaterThanOrEqual(2, $results['total'],
            'Total not greater than or equal to 2 on count_gte filtering');

        $results = $this->client->find('test', [[['count__gte' => 12], ['count__gte' => 2]]]);
        $this->assertGreaterThanOrEqual(1, $results['total'],
            'Total not greater than or equal to 2 on count__gte and count__gte filtering');

        $results = $this->client->find('test', [[['count__lte' => 6], ['count__gte' => 2]], 'title' => 'test4']);
        $this->assertGreaterThanOrEqual(1, $results['total'],
            'Total not greater than or equal to 2 on count__gte and count__gte filtering');

        $results = $this->client->find('test', [[['count__lte' => 8], ['count__gte' => 2]], 'balance__gte' => 55]);
        $this->assertEquals(9, $results['total'],
            'Total not equal to 9 on balance__gte, count__gte and count__gte filtering');

        foreach ($docIds as $docId) {
            $result = $this->client->delete('test', ['id' => $docId]);
            $this->assertTrue($result == 1);
        }
    }

    public function testQueryBuilder()
    {
        $docIds = [];
        $data = $this->bulkData();
        foreach ($data as $d) {
            $docId = $this->client->insert('test', $d);
            $this->assertNotNull($docId, 'Document could not inserted to SQL while testing find');
            if ($docId) {
                $docIds[] = $docId;
            }
        }
        $query = $this->client->query('test');
        $results = $query->andFilter('balance__gte', 55)
            ->orFilter('count__in', [2,3,4,5])
            ->orFilter('balance__gte', 250)
            ->returnFields(['title','date'])
            ->run();
        $this->assertEquals(6, $results['total']);
        $results = $query->andFilter('balance__gte', 55)
            ->orFilter('count__in', [2,3,4,5])
            ->orFilter('balance__gte', 250)
            ->distinctField('balance')
            ->run();
        $this->assertEquals(5, $results['total']);
    }


    public function bulkData()
    {
        return [
            ['date' => '2015-04-10 00:00:00', 'title' => 'test1', 'balance' => 100.0, 'count' => 1],
            ['date' => '2015-04-11 00:00:00', 'title' => 'test2', 'balance' => 120.0, 'count' => 2],
            ['date' => '2015-04-12 00:00:00', 'title' => 'test3', 'balance' => 101.5, 'count' => 3],
            ['date' => '2015-04-12 00:00:00', 'title' => 'test4', 'balance' => 101.5, 'count' => 4],
            ['date' => '2015-04-13 00:00:00', 'title' => 'test5', 'balance' => 150.0, 'count' => 5],
            ['date' => '2015-04-14 00:00:00', 'title' => 'test6', 'balance' => 400.8, 'count' => 6],
            ['date' => '2015-04-15 00:00:00', 'title' => 'test7', 'balance' => 240.0, 'count' => 7],
            ['date' => '2015-04-20 00:00:00', 'title' => 'test8', 'balance' => 760.0, 'count' => 8],
            ['date' => '2015-04-20 00:00:00', 'title' => 'test9', 'balance' => 50.0, 'count' => 9],
            ['date' => '2015-04-21 00:00:00', 'title' => 'test0', 'balance' => 55.5, 'count' => 10],
        ];
    }
}
