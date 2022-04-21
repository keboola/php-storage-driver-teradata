<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Workspace\ResetPassword;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Workspace\ResetWorkspacePasswordCommand;
use Keboola\StorageDriver\Command\Workspace\ResetWorkspacePasswordResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\Password;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class ResetWorkspacePasswordHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param ResetWorkspacePasswordCommand $command
     */
    public function __invoke(
        Message $credentials, // project credentials
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof ResetWorkspacePasswordCommand);

        // validate
        assert($command->getWorkspaceUserName() !== '', 'ResetWorkspacePasswordCommand.workspaceUserName is required');

        $db = $this->manager->createSession($credentials);

        $newWsPassword = Password::generate(
            30,
            Password::SET_LOWERCASE | Password::SET_UPPERCASE | Password::SET_NUMBER | Password::SET_SPECIAL_CHARACTERS
        );

        $db->executeStatement(sprintf(
            'MODIFY USER %s AS PASSWORD = %s FOR USER;',
            TeradataQuote::quoteSingleIdentifier($command->getWorkspaceUserName()),
            TeradataQuote::quoteSingleIdentifier($newWsPassword)
        ));

        // abort existing sessions
        $db->executeStatement(sprintf(
            "SELECT SYSLIB.AbortSessions (-1, %s, 0, 'Y', 'Y');",
            TeradataQuote::quote($command->getWorkspaceUserName())
        ));

        $db->close();

        return (new ResetWorkspacePasswordResponse())
            ->setWorkspacePassword($newWsPassword);
    }
}
