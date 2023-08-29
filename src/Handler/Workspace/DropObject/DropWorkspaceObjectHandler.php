<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Workspace\DropObject;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceObjectCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\DbUtils;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class DropWorkspaceObjectHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param DropWorkspaceObjectCommand $command
     */
    public function __invoke(
        Message $credentials, // workspace credentials
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropWorkspaceObjectCommand);

        assert($runtimeOptions->getRunId() === '');
        assert($runtimeOptions->getMeta() === null);

        // validate
        assert(
            $command->getWorkspaceObjectName() !== '',
            'DropWorkspaceObjectCommand.workspaceObjectName is required'
        );
        assert(
            $command->getObjectNameToDrop() !== '',
            'DropWorkspaceObjectCommand.objectNameToDrop is required'
        );

        $db = $this->manager->createSession($credentials);

        $isTableExists = DbUtils::isTableExists($db, $command->getWorkspaceObjectName(), $command->getObjectNameToDrop());
        if ($command->getIgnoreIfNotExists() && !$isTableExists) {
            $db->close();
            return null;
        }

        $db->executeStatement(sprintf(
            'DROP TABLE %s.%s;',
            TeradataQuote::quoteSingleIdentifier($command->getWorkspaceObjectName()),
            TeradataQuote::quoteSingleIdentifier($command->getObjectNameToDrop())
        ));

        $db->close();
        return null;
    }
}
