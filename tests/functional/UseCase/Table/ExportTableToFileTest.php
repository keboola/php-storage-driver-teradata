<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Doctrine\DBAL\Connection;
use Generator;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\ImportOptions\DedupType;
use Keboola\Db\ImportExport\ImportOptions\ImportType;
use Keboola\Db\ImportExport\Storage\S3\SourceFile;
use Keboola\Db\ImportExport\Storage\Teradata\Table;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Table\ImportExportShared;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\S3Credentials;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Table\Export\ExportTableToFileHandler;
use Keboola\StorageDriver\Teradata\Handler\Table\Import\ImportTableFromFileHandler;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;

class ExportTableToFileTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        [$bucketResponse,] = $this->createTestBucket($projectCredentials, $projectResponse);
        $this->bucketResponse = $bucketResponse;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    /**
     * @dataProvider simpleExportProvider
     */
    public function testExportTableToFile($isCompressed, $exportSize): void
    {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = md5($this->getName()) . '_Test_table_export';
        $exportDir = sprintf(
            'export/%s/',
            str_replace([' ', '"', '\''], ['-', '_', '_'], $this->getName())
        );

        // create table
        $db = $this->getConnection($this->projectCredentials);
        $sourceTableDef = $this->createSourceTable($bucketDatabaseName, $sourceTableName, $db);

        // clear files
        $s3Client = $this->getS3Client(
            (string) getenv('AWS_ACCESS_KEY_ID'),
            (string) getenv('AWS_SECRET_ACCESS_KEY'),
            (string) getenv('AWS_REGION')
        );
        $this->clearS3BucketDir(
            $s3Client,
            (string) getenv('AWS_S3_BUCKET'),
            $exportDir
        );

        // export command
        $cmd = new TableExportToFileCommand();

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new ImportExportShared\Table())
                ->setPath($path)
                ->setTableName($sourceTableName)

        );

        $cmd->setFileProvider(FileProvider::S3);

        $cmd->setFileFormat(FileFormat::CSV);

        $exportOptions = new ExportOptions();
        $exportOptions->setIsCompressed($isCompressed);
        $cmd->setExportOptions($exportOptions);

        $exportMeta = new Any();
        $exportMeta->pack(
            (new TableExportToFileCommand\TeradataTableExportMeta())
                ->setExportAdapter(TableExportToFileCommand\TeradataTableExportMeta\ExportAdapter::TPT)
        );
        $cmd->setMeta($exportMeta);

        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('AWS_S3_BUCKET'))
                ->setPath($exportDir)
        );

        $credentials = new Any();
        $credentials->pack(
            (new S3Credentials())
                ->setKey((string) getenv('AWS_ACCESS_KEY_ID'))
                ->setSecret((string) getenv('AWS_SECRET_ACCESS_KEY'))
                ->setRegion((string) getenv('AWS_REGION'))
        );
        $cmd->setFileCredentials($credentials);

        $handler = new ExportTableToFileHandler($this->sessionManager);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            []
        );

        $this->assertNull($response);

        // check files
        $files = $this->listS3BucketDirFiles(
            $s3Client,
            (string) getenv('AWS_S3_BUCKET'),
            $exportDir
        );
        dump($files);
        $this->assertCount(1, $files);
        $this->assertEquals($exportSize, $files[0]['Size']);

        // cleanup
        $db = $this->getConnection($this->projectCredentials);
        $this->dropSourceTable($sourceTableDef->getSchemaName(), $sourceTableDef->getTableName(), $db);
        $db->close();
    }

    public function simpleExportProvider(): Generator
    {
        yield 'plain csv' => [
            false, // compression
            63, // bytes
        ];
        yield 'gzipped csv' => [
            true, // compression
            46, // bytes
        ];
    }

    private function createSourceTable(
        string $databaseName,
        string $tableName,
        Connection $db
    ): TeradataTableDefinition {
        $tableDef = new TeradataTableDefinition(
            $databaseName,
            $tableName,
            false,
            new ColumnCollection([
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
                TeradataColumn::createGenericColumn('col3'),
            ]),
            []
        );
        $qb = new TeradataTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableDef->getSchemaName(),
            $tableDef->getTableName(),
            $tableDef->getColumnsDefinitions(),
            $tableDef->getPrimaryKeysNames(),
        );
        $db->executeStatement($sql);

        // init some values
        foreach ([
            ['1', '2', '4'],
            ['2', '3', '4'],
            ['3', '3', '3'],
        ] as $i) {
            $db->executeStatement(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                TeradataQuote::quoteSingleIdentifier($databaseName),
                TeradataQuote::quoteSingleIdentifier($tableName),
                implode(',', $i)
            ));
        }

        return $tableDef;
    }

    private function dropSourceTable(
        string $databaseName,
        string $tableName,
        Connection $db
    ): void {
        if (!$this->isTableExists($db, $databaseName, $tableName)) {
            return;
        }
        $qb = new TeradataTableQueryBuilder();
        $db->executeStatement(
            $qb->getDropTableCommand($databaseName, $tableName)
        );
    }

        );
    }
}
