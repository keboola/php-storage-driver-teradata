<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Export;

use Google\Protobuf\Internal\Message;
use Keboola\Db\ImportExport\Backend\Teradata\Exporter;
use Keboola\Db\ImportExport\Storage\ABS;
use Keboola\Db\ImportExport\Storage\DestinationFileInterface;
use Keboola\Db\ImportExport\Storage\S3;
use Keboola\Db\ImportExport\Storage\Teradata\SelectSource;
use Keboola\Db\ImportExport\Storage\Teradata\TeradataExportOptions;
use Keboola\FileStorage\Abs\AbsProvider;
use Keboola\FileStorage\Path\RelativePath;
use Keboola\FileStorage\S3\S3Provider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ABSCredentials;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\S3Credentials;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Command\Table\TableExportToFileResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\Handler\MetaHelper;
use Keboola\StorageDriver\Teradata\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\Teradata\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\Teradata\QueryBuilder\ExportQueryBuilder;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use LogicException;

class ExportTableToFileHandler implements DriverCommandHandlerInterface
{
    public const DEFAULT_BUFFER_SIZE = '5M';
    public const DEFAULT_MAX_OBJECT_SIZE = '50M';

    private TeradataSessionManager $manager;

    public function __construct(
        TeradataSessionManager $manager
    ) {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param TableExportToFileCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof TableExportToFileCommand);

        // validate
        $source = $command->getSource();
        assert($source !== null, 'TableExportToFileCommand.source is required.');
        assert(
            $source->getPath()->count() === 1,
            'TableExportToFileCommand.source.path is required and size must equal 1'
        );
        assert(!empty($source->getTableName()), 'TableExportToFileCommand.source.tableName is required');

        assert(
            in_array($command->getFileProvider(), [FileProvider::S3, FileProvider::ABS], true),
            'Only S3|ABS is supported TableExportToFileCommand.fileProvider.'
        );

        assert(
            $command->getFileFormat() === FileFormat::CSV,
            'Only CSV is supported TableExportToFileCommand.fileFormat.'
        );
        assert($command->getFilePath() !== null, 'TableExportToFileCommand.filePath is required.');

        $any = $command->getFileCredentials();
        assert($any !== null, 'TableExportToFileCommand.fileCredentials is required.');
        $fileCredentials = $any->unpack();
        assert(
            $fileCredentials instanceof S3Credentials || $fileCredentials instanceof ABSCredentials,
            'TableExportToFileCommand.fileCredentials is required to be S3Credentials|ABSCredentials.'
        );

        // validate exportOptions
        $requestExportOptions = $command->getExportOptions() ?? new ExportOptions;

        $exportOptions = $this->createOptions(
            $credentials,
            $requestExportOptions
        );

        $commandMeta = MetaHelper::getMetaFromCommand(
            $command,
            TableExportToFileCommand\TeradataTableExportMeta::class
        );
        assert($commandMeta !== null, 'TableExportToFileCommand.meta is required.');

        // run
        $db = $this->manager->createSession($credentials);

        $databaseName = ProtobufHelper::repeatedStringToArray($source->getPath())[0];
        $columnsDefinitions = (new TeradataTableReflection(
            $db,
            $databaseName,
            $source->getTableName(),
        ))->getColumnsDefinitions();

        $queryBuilder = new ExportQueryBuilder(
            $db,
            new ColumnConverter(),
        );
        $queryData = $queryBuilder->buildQueryFromCommand(
            ExportQueryBuilder::MODE_SELECT,
            $requestExportOptions->getFilters(),
            $requestExportOptions->getOrderBy(),
            $requestExportOptions->getColumnsToExport(),
            $columnsDefinitions,
            $databaseName,
            $source->getTableName(),
            false
        );
        /** @var array<string> $queryDataBindings */
        $queryDataBindings = $queryData->getBindings();
        $sql = $queryBuilder->replaceNamedParametersWithValues(
            $queryData->getQuery(),
            $queryDataBindings,
            $queryData->getTypes(),
        );

        // quote apostrophe - query is a subquery wrapped in apostrophes in TPT script - it need to be fixed
        $sql = str_replace("'", "''", $sql);
        // add semicolon
        $sql .= ';';

        $sourceRef = new SelectSource(
            $sql,
        );
        $destinationRef = $this->getDestinationFile($command->getFilePath(), $fileCredentials);

        (new Exporter($db))->exportTable(
            $sourceRef,
            $destinationRef,
            $exportOptions
        );

        return (new TableExportToFileResponse())
            ->setTableInfo(TableReflectionResponseTransformer::transformTableReflectionToResponse(
                $databaseName,
                new TeradataTableReflection(
                    $db,
                    $databaseName,
                    $source->getTableName()
                )
            ));
    }

    /**
     * @param S3Credentials|ABSCredentials $fileCredentials
     */
    private function getDestinationFile(
        FilePath $filePath,
        Message $fileCredentials
    ): DestinationFileInterface {
        if ($fileCredentials instanceof S3Credentials) {
            $relativePath = RelativePath::create(
                new S3Provider(),
                $filePath->getRoot(),
                $filePath->getPath(),
                $filePath->getFileName(),
            );
            return new S3\DestinationFile(
                $fileCredentials->getKey(),
                $fileCredentials->getSecret(),
                $fileCredentials->getRegion(),
                $relativePath->getRoot(),
                $relativePath->getPathnameWithoutRoot()
            );
        }

        if ($fileCredentials instanceof AbsCredentials) {
            $relativePath = RelativePath::create(
                new AbsProvider(),
                $filePath->getRoot(),
                $filePath->getPath(),
                $filePath->getFileName(),
            );
            return new ABS\DestinationFile(
                $filePath->getRoot(),
                $relativePath->getPathnameWithoutRoot(),
                $fileCredentials->getSasToken(),
                $fileCredentials->getAccountName(),
                $fileCredentials->getAccountKey(),
            );
        }

        /** @phpstan-ignore-next-line safety check */
        throw new LogicException('Unknown storage of destination file.');
    }

    private function createOptions(
        GenericBackendCredentials $credentials,
        ?ExportOptions $options
    ): TeradataExportOptions {
        return new TeradataExportOptions(
            $credentials->getHost(),
            $credentials->getPrincipal(),
            $credentials->getSecret(),
            $credentials->getPort(),
            $options && $options->getIsCompressed(),
            self::DEFAULT_BUFFER_SIZE,
            self::DEFAULT_MAX_OBJECT_SIZE
        );
    }
}
