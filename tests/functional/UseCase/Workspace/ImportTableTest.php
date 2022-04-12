<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Import\ImportTableFromTableHandler;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

class ImportTableTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    protected GenericBackendCredentials $workspaceCredentials;

    protected CreateWorkspaceResponse $workspaceResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestWorkspace();
        $this->cleanTestProject();

        // create project
        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        // create bucket
        [$bucketResponse,] = $this->createTestBucket($projectCredentials, $projectResponse);
        $this->bucketResponse = $bucketResponse;

        // create workspace
        [
            $workspaceCredentials,
            $workspaceResponse,
        ] = $this->createTestWorkspace($this->projectCredentials, $projectResponse);
        $this->workspaceCredentials = $workspaceCredentials;
        $this->workspaceResponse = $workspaceResponse;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestWorkspace();
        $this->cleanTestProject();
    }

    public function testImportTableToWorkspace(): void
    {
        $sourceTableName = $this->createTableInBucket();

        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $db = $this->getConnection($this->projectCredentials);

        $qb = new TeradataTableQueryBuilder();
        $tableSourceRef = new TeradataTableReflection($db, $bucketDatabaseName, $sourceTableName);
        /** @var TeradataTableDefinition $tableSourceDef */
        $tableSourceDef = $tableSourceRef->getTableDefinition();

        foreach ([['1', '\'name\'', '\'1\''], ['2', null, '\'large2\''], ['3', '\'3\'', '\'large3\'']] as $i) {
            $db->executeStatement(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier($sourceTableName),
                implode(',', $i)
            ));
        }
        // create destination in workspace as WS user
        $wsConn = $this->getConnection($this->workspaceCredentials);
        $tableDestDef = new TeradataTableDefinition(
            $this->workspaceResponse->getWorkspaceObjectName(),
            $destinationTableName,
            false,
            new ColumnCollection([
                TeradataColumn::createGenericColumn('id'),
                TeradataColumn::createGenericColumn('col1'), // <- different col rename
                TeradataColumn::createGenericColumn('large'),
            ]),
            []
        );
        $sql = $qb->getCreateTableCommandFromDefinition($tableDestDef);
        $wsConn->executeStatement($sql);
        // ws user can read directly from source in storage
        $ref = new TeradataTableReflection($wsConn, $tableSourceDef->getSchemaName(), $tableSourceDef->getTableName());
        $this->assertSame(3, $ref->getRowsCount());

        $cmd = new TableImportFromTableCommand();
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class
        );
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('id')
            ->setDestinationColumnName('id');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('name')
            ->setDestinationColumnName('col1');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('large')
            ->setDestinationColumnName('large');

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $tableSourceDef->getSchemaName();
        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
                ->setColumnMappings($columnMappings)
        );

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $tableDestDef->getSchemaName();
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
        );

        $handler = new ImportTableFromTableHandler($this->sessionManager);
        $handler(
            $this->projectCredentials,
            $cmd,
            []
        );

        $ref = new TeradataTableReflection($wsConn, $tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $this->assertSame(3, $ref->getRowsCount());

        // cleanup
        $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName());
        $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $db->close();
    }

    private function createTableInBucket(): string
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->sessionManager);

        $metaIsLatinEnabled = new Any();
        $metaIsLatinEnabled->pack(
            (new CreateTableCommand\TableColumn\TeradataTableColumnMeta())->setIsLatin(true)
        );

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columns = new RepeatedField(GPBType::MESSAGE, CreateTableCommand\TableColumn::class);
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('id')
            ->setType(Teradata::TYPE_INTEGER);
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('name')
            ->setType(Teradata::TYPE_VARCHAR)
            ->setLength('50')
            ->setNullable(true)
            ->setDefault("'Some Default'");
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('large')
            ->setType(Teradata::TYPE_VARCHAR)
            ->setLength('10000')
            ->setMeta($metaIsLatinEnabled);
        $primaryKeysNames = new RepeatedField(GPBType::STRING);
        $primaryKeysNames[] = 'id';
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns)
            ->setPrimaryKeysNames($primaryKeysNames);

        $handler(
            $this->projectCredentials,
            $command,
            []
        );
        return $tableName;
    }
}
