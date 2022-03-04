<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Drop\DropTableHandler;

class CreateDropTableTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        [$bucketResponse, $connection] = $this->createTestBucket($projectCredentials, $projectResponse);
        $this->bucketResponse = $bucketResponse;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testCreateTable(): void
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // TODO je potreba nastavit jinou databazi? Pokud jo, tak potom je zbytecne nastavovat command.path.
        // use bucket database
        //$meta = new Any();
        //$meta->pack(
        //    (new GenericBackendCredentials\TeradataCredentialsMeta())
        //        ->setDatabase($bucketDatabaseName)
        //);
        //$bucketCredentials = (new GenericBackendCredentials())
        //    ->setHost($this->projectCredentials->getHost())
        //    ->setPrincipal($this->projectCredentials->getPrincipal())
        //    ->setSecret($this->projectCredentials->getSecret())
        //    ->setPort($this->projectCredentials->getPort())
        //    ->setMeta($meta);

        // CREATE TABLE
        $handler = new CreateTableHandler();

        $metaIsLatinEnabled = new Any();
        $metaIsLatinEnabled->pack((new CreateTableCommand\TableColumn\TeradataTableColumnMeta())->setIsLatin(true));

        // TODO phpstan: jak nastavit repeated?
        // ERR Parameter #1 $var of method Keboola\StorageDriver\Command\Table\CreateTableCommand::setPath()
        //     expects Google\Protobuf\Internal\RepeatedField&iterable<string>, array<int, string> given.
        $path = [$bucketDatabaseName];
        // ERR Parameter #2 $value of method Google\Protobuf\Internal\RepeatedField::offsetSet() expects object,
        //     string given.
        //$path = new RepeatedField(GPBType::STRING);
        //$path[] = $bucketDatabaseName;
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns([
                (new CreateTableCommand\TableColumn())
                    ->setName('id')
                    ->setType(Teradata::TYPE_INTEGER),
                (new CreateTableCommand\TableColumn())
                    ->setName('name')
                    ->setType(Teradata::TYPE_VARCHAR)
                    ->setLength('50')
                    ->setNullable(true)
                    ->setDefault("'Some Default'")
                    ->setMeta($metaIsLatinEnabled),
            ])
            ->setPrimaryKeysNames(['id']);

        $response = $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $this->assertNull($response);

        $db = $this->getConnection($this->projectCredentials);

        $this->assertTrue($this->isTableExists($db, $bucketDatabaseName, $tableName));

        $db->close();

        // DROP TABLE
        $handler = new DropTableHandler();
        $command = (new DropTableCommand())
            ->setPath([$bucketDatabaseName])
            ->setTableName($tableName);

        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $db = $this->getConnection($this->getCredentials());

        $this->assertFalse($this->isTableExists($db, $bucketDatabaseName, $tableName));

        $db->close();
    }
}
