<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Doctrine\DBAL\Connection;
use Generator;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\CsvOptions\CsvOptions;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Info\TableReflection;
use Keboola\StorageDriver\Command\Table\ImportExportShared;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\S3Credentials;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Command\Table\TableExportToFileResponse;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
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
    public function testExportTableToFile(bool $isCompressed, int $exportSize): void
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

        $this->assertInstanceOf(TableExportToFileResponse::class, $response);

        $exportedTableInfo = $response->getTableInfo();
        $this->assertNotNull($exportedTableInfo);

        $this->assertSame($sourceTableName, $exportedTableInfo->getTableName());
        $this->assertSame([$bucketDatabaseName], ProtobufHelper::repeatedStringToArray($exportedTableInfo->getPath()));
        $this->assertSame(
            $sourceTableDef->getPrimaryKeysNames(),
            ProtobufHelper::repeatedStringToArray($exportedTableInfo->getPrimaryKeysNames())
        );
        /** @var TableReflection\TableColumn[] $columns */
        $columns = iterator_to_array($exportedTableInfo->getColumns()->getIterator());
        $columnsNames = array_map(
            static fn(TableReflection\TableColumn $col) => $col->getName(),
            $columns
        );
        $this->assertSame($sourceTableDef->getColumnsNames(), $columnsNames);

        // check files
        /** @var array<int, array<string, mixed>> $files */
        $files = $this->listS3BucketDirFiles(
            $s3Client,
            (string) getenv('AWS_S3_BUCKET'),
            $exportDir
        );
        $this->assertNotNull($files);
        $this->assertCount(1, $files);
        $this->assertEquals($exportSize, $files[0]['Size']);

        // cleanup
        $db = $this->getConnection($this->projectCredentials);
        $this->dropSourceTable($sourceTableDef->getSchemaName(), $sourceTableDef->getTableName(), $db);
        $db->close();
    }

    /**
     * @dataProvider slicedExportProvider
     * @param array<int, array<string, mixed>> $expectedFiles
     */
    public function testExportTableToSlicedFile(bool $isCompressed, array $expectedFiles): void
    {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = md5($this->getName()) . '_Test_table_export_sliced';
        $exportDir = sprintf(
            'export/%s/',
            md5($this->getName())
        );

        // cleanup
        $db = $this->getConnection($this->projectCredentials);
        $this->dropSourceTable($bucketDatabaseName, $sourceTableName, $db);
        $db->close();

        // create table from file
        $this->createSourceTableFromFile(
            $db,
            'export',
            'big_table.csv.gz',
            true,
            $bucketDatabaseName,
            $sourceTableName,
            [
                'FID',
                'NAZEV',
                'Y',
                'X',
                'KONTAKT',
                'SUBKATEGORIE',
                'KATEGORIE',
                'Column6',
                'Column7',
                'Column8',
                'Column9',
                'GlobalID',
            ]
        );

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

        $this->assertInstanceOf(TableExportToFileResponse::class, $response);

        // check files
        $files = $this->listS3BucketDirFiles(
            $s3Client,
            (string) getenv('AWS_S3_BUCKET'),
            $exportDir
        );
        $this->assertNotNull($files);
        self::assertFilesMatch($expectedFiles, $files);

        // cleanup
        $db = $this->getConnection($this->projectCredentials);
        $this->dropSourceTable($bucketDatabaseName, $sourceTableName, $db);
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

    public function slicedExportProvider(): Generator
    {
        yield 'plain csv' => [
            false, // compression
            [
                ['fileName' => 'F00000', 'size' => 48],
                ['fileName' => 'F00001', 'size' => 43],
            ],
        ];
        yield 'gzipped csv' => [
            true, // compression
            [
                ['fileName' => 'F00000', 'size' => 0],
            ],
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

    /**
     * @param string[] $sourceColumns
     */
    private function createSourceTableFromFile(
        Connection $db,
        string $sourceFilePath,
        string $sourceFileName,
        bool $sourceFileIsCompressed,
        string $destinationDatabaseName,
        string $destinationTableName,
        array $sourceColumns
    ): void {
        // create table
        $columnsLines = [];
        foreach ($sourceColumns as $column) {
            $columnsLines[] = sprintf(
                '%s VARCHAR(500) CHARACTER SET UNICODE',
                $column
            );
        }
        $db->executeQuery(
            sprintf(
                'CREATE MULTISET TABLE %s.%s (
                    %s
                );',
                TeradataQuote::quoteSingleIdentifier($destinationDatabaseName),
                TeradataQuote::quoteSingleIdentifier($destinationTableName),
                implode(",\n", $columnsLines)
            )
        );
        $db->close();

        // import data to table
        $cmd = new TableImportFromFileCommand();
        $cmd->setFileProvider(FileProvider::S3);
        $cmd->setFileFormat(FileFormat::CSV);

        $columns = new RepeatedField(GPBType::STRING);
        foreach ($sourceColumns as $column) {
            $columns[] = $column;
        }
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SINGLE_FILE)
                ->setCompression($sourceFileIsCompressed
                    ? TableImportFromFileCommand\CsvTypeOptions\Compression::GZIP
                    : TableImportFromFileCommand\CsvTypeOptions\Compression::NONE)
        );
        $cmd->setFormatTypeOptions($formatOptions);

        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('AWS_S3_BUCKET'))
                ->setPath($sourceFilePath)
                ->setFileName($sourceFileName)
        );

        $credentials = new Any();
        $credentials->pack(
            (new S3Credentials())
                ->setKey((string) getenv('AWS_ACCESS_KEY_ID'))
                ->setSecret((string) getenv('AWS_SECRET_ACCESS_KEY'))
                ->setRegion((string) getenv('AWS_REGION'))
        );
        $cmd->setFileCredentials($credentials);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $destinationDatabaseName;
        $cmd->setDestination(
            (new ImportExportShared\Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );

        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportExportShared\ImportOptions())
                ->setImportType(ImportExportShared\ImportOptions\ImportType::FULL)
                ->setDedupType(ImportExportShared\ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
        );

        $meta = new Any();
        $meta->pack(
            (new TableImportFromFileCommand\TeradataTableImportMeta())
                ->setImportAdapter(TableImportFromFileCommand\TeradataTableImportMeta\ImportAdapter::TPT)
        );
        $cmd->setMeta($meta);

        $handler = new ImportTableFromFileHandler($this->sessionManager);
        $handler(
            $this->projectCredentials,
            $cmd,
            []
        );
    }

    /**
     * @param array<int, array<string, mixed>> $expectedFiles
     * @param array<int, array<string, mixed>> $files
     */
    public static function assertFilesMatch(array $expectedFiles, array $files): void
    {
        self::assertCount(count($expectedFiles), $files);
        /** @var array{fileName: string, size: int} $expectedFile */
        foreach ($expectedFiles as $i => $expectedFile) {
            /** @var array{Key: string, Size: string} $actualFile */
            $actualFile = $files[$i];
            self::assertStringContainsString((string) $expectedFile['fileName'], (string) $actualFile['Key']);
            $fileSize = (int) $actualFile['Size'];
            $expectedFileSize = ((int) $expectedFile['size']) * 1024 * 1024;
            // check that the file size is in range xMB +- 1 000 000B
            //  - (because I cannot really say what the exact size in bytes should be)
            if ($expectedFileSize !== 0) {
                self::assertTrue(
                    ($expectedFileSize - 1000000) < $fileSize && $fileSize < ($expectedFileSize + 100000),
                    sprintf('Actual size is %s but expected is %s', $fileSize, $expectedFileSize)
                );
            }
        }
    }
}
