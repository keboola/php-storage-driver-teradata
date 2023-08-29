<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Alter;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsResponse;
use Keboola\StorageDriver\Teradata\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\Teradata\QueryBuilder\ExportQueryBuilder;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportFilters;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

final class DeleteTableRowsHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param DeleteTableRowsCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DeleteTableRowsCommand);

        assert($command->getPath()->count() === 1, 'AddColumnCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'AddColumnCommand.tableName is required');

        assert($runtimeOptions->getRunId() === '');
        assert($runtimeOptions->getMeta() === null);

        $db = $this->manager->createSession($credentials);
        /** @var string $datasetName */
        $datasetName = $command->getPath()[0];

        $this->validateFilters($command);

        // build sql
        $queryBuilder = new ExportQueryBuilder($db, new ColumnConverter());
        $ref = new TeradataTableReflection($db, $datasetName, $command->getTableName());
        $tableColumnsDefinitions = $ref->getColumnsDefinitions();

        $queryData = $queryBuilder->buildQueryFromCommand(
            ExportQueryBuilder::MODE_DELETE,
            (new ExportFilters())
                ->setChangeSince($command->getChangeSince())
                ->setChangeUntil($command->getChangeUntil())
                ->setWhereFilters($command->getWhereFilters()),
            new RepeatedField(GPBType::MESSAGE, ExportOrderBy::class),
            new RepeatedField(GPBType::STRING),
            $tableColumnsDefinitions,
            $datasetName,
            $command->getTableName(),
            false
        );

        $deletedRowsCount = $db->executeStatement(
            $queryData->getQuery(),
            $queryData->getBindings(),
            $queryData->getTypes(),
        );

        $stats = $ref->getTableStats();
        return (new DeleteTableRowsResponse())
            ->setDeletedRowsCount((int) $deletedRowsCount)
            ->setTableRowsCount($stats->getRowsCount())
            ->setTableSizeBytes($stats->getDataSizeBytes());
    }

    private function validateFilters(DeleteTableRowsCommand $command): void
    {
        if ($command->getChangeSince() !== '') {
            assert(
                is_numeric($command->getChangeSince()),
                'PreviewTableCommand.changeSince must be numeric timestamp'
            );
        }
        if ($command->getChangeUntil() !== '') {
            assert(
                is_numeric($command->getChangeUntil()),
                'PreviewTableCommand.changeUntil must be numeric timestamp'
            );
        }
    }
}
