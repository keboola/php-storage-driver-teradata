<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Alter;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Table\AddColumnCommand;
use Keboola\StorageDriver\Command\Table\AddPrimaryKeyCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

final class AddPKHandler implements DriverCommandHandlerInterface
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
        array   $features
    ): ?Message
    {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof AddPrimaryKeyCommand);

        // validate
        assert($command->getPath()->count() === 1, 'AddPrimaryKeyCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'AddPrimaryKeyCommand.tableName is required');
        assert($command->getPrimaryKeysNames()->count() < 1, 'AddPrimaryKeyCommand.primaryKeysNames is required and cannot be empty');

        $desiredPks = $command->getPrimaryKeysNames();
        $dbName = $command->getPath()[0];
        try {
            $db = $this->manager->createSession($credentials);

            $qb = new TeradataTableQueryBuilder();
            $duplicatesCheckSql = $qb->getCommandForDuplicates($dbName, $command->getTableName(), $desiredPks);

            $maxRowsForPKs = $db->fetchOne($duplicatesCheckSql);
            if ((int) $maxRowsForPKs > 1) {
                throw CannotAddPrimaryKeyException::createForDuplicates();
            }

            $reflection = new TeradataTableReflection($db, $dbName, $command->getTableName());

            if ($reflection->getPrimaryKeysNames() !== []) {
                throw CannotAddPrimaryKeyException::createForExistingPK();
            }

            /** @var TeradataColumn $columnDefinition */
            foreach ($reflection->getColumnsDefinitions() as $columnDefinition) {
                if (in_array($columnDefinition->getColumnName(), $desiredPks) && $columnDefinition->getColumnDefinition()->isNullable()) {
                    throw CannotAddPrimaryKeyException::createForNullableColumn($columnDefinition->getColumnName());
                }
            }

            $addPKSQL = $qb->getAddPrimaryKeyCommand($dbName, $command->getTableName(), $desiredPks);
            $db->executeStatement($addPKSQL);
        } finally {
            if (isset($db)) {
                $db->close();
            }
        }

        return null;
    }
}
