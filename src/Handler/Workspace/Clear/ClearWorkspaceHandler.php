<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Workspace\Clear;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Workspace\ClearWorkspaceCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Throwable;

final class ClearWorkspaceHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param ClearWorkspaceCommand $command
     */
    public function __invoke(
        Message $credentials, // project credentials
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof ClearWorkspaceCommand);

        // validate
        assert(!empty($command->getWorkspaceObjectName()), 'ClearWorkspaceCommand.workspaceObjectName is required');

        $db = $this->manager->createSession($credentials);

        try {
            $db->executeStatement(sprintf(
                'DELETE DATABASE %s ALL',
                TeradataQuote::quoteSingleIdentifier($command->getWorkspaceObjectName())
            ));
        } catch (Throwable $e) {
            if (!$command->getIgnoreErrors()) {
                $db->close();
                throw $e;
            }
        }

        $db->close();
        return null;
    }
}
