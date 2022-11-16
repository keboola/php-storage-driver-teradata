<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Create;

use Google\Protobuf\Internal\Message;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\CreateTableCommand\TableColumn;
use Keboola\StorageDriver\Command\Table\CreateTableCommand\TableColumn\TeradataTableColumnMeta;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\MetaHelper;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

final class CreateTableHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param CreateTableCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateTableCommand);

        // validate
        assert($command->getPath()->count() === 1, 'CreateTableCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'CreateTableCommand.tableName is required');
        assert($command->getColumns()->count() > 0, 'CreateTableCommand.columns is required');

        try {
            $db = $this->manager->createSession($credentials);

            // define columns
            $columns = [];
            /** @var TableColumn $column */
            foreach ($command->getColumns() as $column) {
                // validate
                assert($column->getName() !== '', 'TableColumn.name is required');
                assert($column->getType() !== '', 'TableColumn.type is required');

                /** @var TeradataTableColumnMeta|null $columnMeta */
                $columnMeta = MetaHelper::getMetaRestricted($column, TeradataTableColumnMeta::class);

                $columnDefinition = new Teradata($column->getType(), [
                    'length' => $column->getLength() === '' ? null : $column->getLength(),
                    'nullable' => $column->getNullable(),
                    'default' => $column->getDefault() === '' ? null : $column->getDefault(),
                    'isLatin' => $columnMeta ? $columnMeta->getIsLatin() : false,
                ]);
                $columns[] = new TeradataColumn($column->getName(), $columnDefinition);
            }
            $columnsCollection = new ColumnCollection($columns);

            // build sql
            $builder = new TeradataTableQueryBuilder();
            /** @var string $databaseName */
            $databaseName = $command->getPath()[0];
            $primaryKeys = ProtobufHelper::repeatedStringToArray($command->getPrimaryKeysNames());
            $createTableSql = $builder->getCreateTableCommand(
                $databaseName,
                $command->getTableName(),
                $columnsCollection,
                $primaryKeys
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
