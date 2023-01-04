<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Alter;

use Google\Protobuf\Internal\Message;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Table\AddColumnCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Command\Table\TableColumnShared\TeradataTableColumnMeta;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\MetaHelper;
use Keboola\StorageDriver\Teradata\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

final class AddColumnHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param AddColumnCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof AddColumnCommand);

        $column = $command->getColumnDefinition();
        // validate
        assert($command->getPath()->count() === 1, 'AddColumnCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'AddColumnCommand.tableName is required');
        assert($column instanceof TableColumnShared, 'AddColumnCommand.columnDefinition is required');

        try {
            $db = $this->manager->createSession($credentials);

            // define columns
            // validate
            assert($column->getName() !== '', 'TableColumnShared.name is required');
            assert($column->getType() !== '', 'TableColumnShared.type is required');

            /** @var TeradataTableColumnMeta|null $columnMeta */
            $columnMeta = MetaHelper::getMetaRestricted($column, TeradataTableColumnMeta::class);

            $columnDefinition = new TeradataColumn(
                $column->getName(),
                new Teradata($column->getType(), [
                    'length' => $column->getLength() === '' ? null : $column->getLength(),
                    'nullable' => $column->getNullable(),
                    'default' => $column->getDefault() === '' ? null : $column->getDefault(),
                    'isLatin' => $columnMeta ? $columnMeta->getIsLatin() : false,
                ])
            );

            // build sql
            $builder = new TeradataTableQueryBuilder();
            /** @var string $databaseName */
            $databaseName = $command->getPath()[0];
            $createTableSql = $builder->getAddColumnCommand(
                $databaseName,
                $command->getTableName(),
                $columnDefinition
            );

            // create table
            $db->executeStatement($createTableSql);

            $response = (new ObjectInfoResponse())
                ->setPath($command->getPath())
                ->setObjectType(ObjectType::TABLE)
                ->setTableInfo(TableReflectionResponseTransformer::transformTableReflectionToResponse(
                    $databaseName,
                    new TeradataTableReflection(
                        $db,
                        $databaseName,
                        $command->getTableName()
                    )
                ));
        } finally {
            if (isset($db)) {
                $db->close();
            }
        }

        return $response;
    }
}
