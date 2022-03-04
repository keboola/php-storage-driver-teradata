<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Create;

use Google\Protobuf\Internal\Message;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\CreateTableCommand\TableColumn;
use Keboola\StorageDriver\Command\Table\CreateTableCommand\TableColumn\TeradataTableColumnMeta;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\MetaHelper;
use Keboola\StorageDriver\Teradata\ConnectionFactory;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;

final class CreateTableHandler implements DriverCommandHandlerInterface
{
    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateTableCommand);

        // validate
        assert($command->getPath()->count() > 0, 'CreateTableCommand.path is required');
        assert(!empty($command->getTableName()), 'CreateTableCommand.tableName is required');
        assert($command->getColumns()->count() > 0, 'CreateTableCommand.columns is required');

        // get bucket database
        $paths = [];
        foreach ($command->getPath() as $path) {
            $paths[] = $path;
        }
        $databaseName = implode('.', $paths);

        $db = ConnectionFactory::getConnection($credentials);

        // define columns
        $columns = [];
        /** @var TableColumn $column */
        foreach ($command->getColumns() as $column) {
            // validate
            assert(!empty($column->getName()), 'TableColumn.name is required');
            assert(!empty($column->getType()), 'TableColumn.type is required');

            /** @var TeradataTableColumnMeta|null $columnMeta */
            $columnMeta = MetaHelper::getMetaRestricted($column, TeradataTableColumnMeta::class);

            $columnDefinition = new Teradata($column->getType(), [
                'length' => $column->getLength(),
                'nullable' => $column->getNullable(),
                'default' => $column->getDefault() === '' ? null : $column->getDefault(),
                'isLatin' => $columnMeta ? $columnMeta->getIsLatin() : false,
            ]);
            $columns[] = new TeradataColumn($column->getName(), $columnDefinition);
        }
        $columnsCollection = new ColumnCollection($columns);

        // build sql
        $builder = new TeradataTableQueryBuilder();
        // convert RepeatedField to Array: https://github.com/protocolbuffers/protobuf/issues/7648
        $primaryKeys = [];
        foreach ($command->getPrimaryKeysNames() as $primaryKey) {
            $primaryKeys[] = $primaryKey;
        }
        $createTableSql = $builder->getCreateTableCommand(
            $databaseName,
            $command->getTableName(),
            $columnsCollection,
            $primaryKeys
        );

        // create table
        $db->executeStatement($createTableSql);

        $db->close();
        return null;
    }
}
