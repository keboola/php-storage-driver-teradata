<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Table\AddPrimaryKeyCommand;
use Keboola\StorageDriver\Command\Table\DropPrimaryKeyCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Table\Alter\AddPKHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Alter\DropPKHandler;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

class PrimaryKeyTest extends BaseCase
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

    public function testAddDropPK(): void
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $tableStructure = [
            'columns' => [
                'col1' => [
                    'type' => Teradata::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'col2' => [
                    'type' => Teradata::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'col3' => [
                    'type' => Teradata::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
            ],
            'primaryKeysNames' => [],
        ];
        $this->createTable($bucketDatabaseName, $tableName, $tableStructure);
        $this->fillTableWithData($bucketDatabaseName, $tableName, ['columns' => ['col1', 'col2', 'col3'], 'rows'=> ['1,2,3', '4,5,6']]);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $pkNames = new RepeatedField(GPBType::STRING);
        $pkNames[] = 'col2';
        $pkNames[] = 'col3';

        // add PK
        $addPKCommand = (new AddPrimaryKeyCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setPrimaryKeysNames($pkNames);
        $addPKHandler = new AddPKHandler($this->sessionManager);
        $addPKHandler(
            $this->projectCredentials,
            $addPKCommand,
            []
        );

        // check the existence of PK via table reflection
        $db = $this->getConnection($this->projectCredentials);
        $ref = new TeradataTableReflection($db, $bucketDatabaseName, $tableName);
        $this->assertEquals(['col2', 'col3'], $ref->getPrimaryKeysNames());

        // drop PK
        $dropPKCommand = (new DropPrimaryKeyCommand())
            ->setPath($path)
            ->setTableName($tableName);
        $dropPKHandler = new DropPKHandler($this->sessionManager);
        $dropPKHandler(
            $this->projectCredentials,
            $dropPKCommand,
            []
        );

        // check the non-existence of PK via table reflection
        $db = $this->getConnection($this->projectCredentials);
        $ref = new TeradataTableReflection($db, $bucketDatabaseName, $tableName);
        $this->assertEquals([], $ref->getPrimaryKeysNames());
    }
}
