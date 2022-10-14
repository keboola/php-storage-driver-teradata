<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Backend;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Common\TerminateSessionCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;

final class TerminateSessionHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

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
        assert($command instanceof TerminateSessionCommand);

        $db = $this->manager->createSession($credentials);

        // abort existing sessions
        $db->executeStatement(sprintf(
            "SELECT SYSLIB.AbortSessions (-1, '*', %s, 'Y', 'Y');",
            $command->getSessionId()
        ));

        return null;
    }
}
