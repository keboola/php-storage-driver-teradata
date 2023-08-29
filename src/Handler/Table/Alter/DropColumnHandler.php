<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Alter;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Table\DropColumnCommand;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;

final class DropColumnHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param DropColumnCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropColumnCommand);

        assert($command->getPath()->count() === 1, 'DropColumnCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'DropColumnCommand.tableName is required');

        assert($runtimeOptions->getRunId() === '');
        assert($runtimeOptions->getMeta() === null);

        try {
            $db = $this->manager->createSession($credentials);

            // define columns
            // validate
            assert($command->getColumnName() !== '', 'DropColumnCommand.columnName is required');

            // build sql
            $builder = new TeradataTableQueryBuilder();
            /** @var string $databaseName */
            $databaseName = $command->getPath()[0];
            $dropColumnSql = $builder->getDropColumnCommand(
                $databaseName,
                $command->getTableName(),
                $command->getColumnName()
            );

            // create table
            $db->executeStatement($dropColumnSql);

        } finally {
            if (isset($db)) {
                $db->close();
            }
        }

        return null;
    }
}
