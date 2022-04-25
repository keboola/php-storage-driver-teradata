<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Workspace\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Throwable;

final class DropWorkspaceHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param DropWorkspaceCommand $command
     */
    public function __invoke(
        Message $credentials, // project credentials
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropWorkspaceCommand);

        // validate
        assert($command->getWorkspaceUserName() !== '', 'DropWorkspaceCommand.workspaceUserName is required');
        assert($command->getWorkspaceRoleName() !== '', 'DropWorkspaceCommand.workspaceRoleName is required');
        assert($command->getWorkspaceObjectName() !== '', 'DropWorkspaceCommand.workspaceObjectName is required');

        $db = $this->manager->createSession($credentials);

        if ($command->getIsCascade()) {
            $db->executeStatement(sprintf(
                'DELETE DATABASE %s ALL',
                TeradataQuote::quoteSingleIdentifier($command->getWorkspaceObjectName())
            ));
        }

        $db->executeStatement(sprintf(
            'DROP USER %s;',
            TeradataQuote::quoteSingleIdentifier($command->getWorkspaceUserName())
        ));

        $db->executeStatement(sprintf(
            'DROP ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($command->getWorkspaceRoleName())
        ));

        $db->close();
        return null;
    }
}
