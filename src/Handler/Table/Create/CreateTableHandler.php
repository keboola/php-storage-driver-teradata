<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Create;

use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Teradata;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\CreateTableCommand\TableColumn;
use Keboola\StorageDriver\Command\Table\CreateTableCommand\TableColumn\TeradataTableColumnMeta;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\MetaHelper;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;

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
        assert(!empty($command->getTableName()), 'CreateTableCommand.tableName is required');
        assert($command->getColumns()->count() > 0, 'CreateTableCommand.columns is required');

        $db = $this->manager->createSession($credentials);

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
        /** @var string $databaseName */
        $databaseName = $command->getPath()[0];
        $primaryKeys = $this->repeatedStringToArray($command->getPrimaryKeysNames());
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

    /**
     * Convert RepeatedField to Array: https://github.com/protocolbuffers/protobuf/issues/7648
     *
     * @return string[]
     */
    private function repeatedStringToArray(RepeatedField $repeated): array
    {
        $values = [];
        foreach ($repeated as $value) {
            $values[] = strval($value);
        }
        return $values;
    }
}
