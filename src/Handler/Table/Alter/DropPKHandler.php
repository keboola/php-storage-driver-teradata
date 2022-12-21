<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Alter;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Table\DropPrimaryKeyCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;

final class DropPKHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param DropPrimaryKeyCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropPrimaryKeyCommand);

        // validate
        assert($command->getPath()->count() === 1, 'DropPrimaryKeyCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'DropPrimaryKeyCommand.tableName is required');

        $dbName = $command->getPath()[0];
        try {
            $db = $this->manager->createSession($credentials);

            $qb = new TeradataTableQueryBuilder();

            $addPKSQL = $qb->getDropPrimaryKeyCommand($dbName, $command->getTableName());
            $db->executeStatement($addPKSQL);
        } finally {
            if (isset($db)) {
                $db->close();
            }
        }

        return null;
    }
}
