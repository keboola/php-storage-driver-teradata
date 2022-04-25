<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Workspace\Create;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\BackendSupportsInterface;
use Keboola\StorageDriver\Shared\Driver\MetaHelper;
use Keboola\StorageDriver\Shared\NameGenerator\NameGeneratorFactory;
use Keboola\StorageDriver\Shared\Utils\Password;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class CreateWorkspaceHandler implements DriverCommandHandlerInterface
{
    public const DEFAULT_PERM_SPACE_SIZE = 1e8; // 100MB
    public const DEFAULT_SPOOL_SPACE_SIZE = 1e8; // 100MB

    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param CreateWorkspaceCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateWorkspaceCommand);

        // validate
        assert($command->getStackPrefix() !== '', 'CreateWorkspaceCommand.stackPrefix is required');
        assert($command->getWorkspaceId() !== '', 'CreateWorkspaceCommand.workspaceId is required');
        assert($command->getProjectUserName() !== '', 'CreateWorkspaceCommand.projectUserName is required');
        assert($command->getProjectRoleName() !== '', 'CreateWorkspaceCommand.projectRoleName is required');
        assert(
            $command->getProjectReadOnlyRoleName() !== '',
            'CreateWorkspaceCommand.projectReadOnlyRoleName is required',
        );

        $db = $this->manager->createSession($credentials);

        // root user is also root database
        $databaseName = $credentials->getPrincipal();

        // allow override spaces for user
        /** @var CreateWorkspaceCommand\CreateWorkspaceTeradataMeta|null $meta */
        $meta = MetaHelper::getMetaRestricted($command, CreateWorkspaceCommand\CreateWorkspaceTeradataMeta::class);
        $permSpace = self::DEFAULT_PERM_SPACE_SIZE;
        $spoolSpace = self::DEFAULT_SPOOL_SPACE_SIZE;
        if ($meta !== null) {
            $permSpace = $meta->getPermSpace() !== '' ? $meta->getPermSpace() : $permSpace;
            $spoolSpace = $meta->getSpoolSpace() !== '' ? $meta->getSpoolSpace() : $spoolSpace;
        }

        $nameGenerator = NameGeneratorFactory::getGeneratorForBackendAndPrefix(
            BackendSupportsInterface::BACKEND_TERADATA,
            $command->getStackPrefix()
        );
        $newWsUserName = $nameGenerator->createWorkspaceUserNameForWorkspaceId($command->getWorkspaceId());
        $newWsRoleName = $nameGenerator->createWorkspaceRoleNameForWorkspaceId($command->getWorkspaceId());
        $newWsPassword = Password::generate(
            30,
            Password::SET_LOWERCASE | Password::SET_UPPERCASE | Password::SET_NUMBER | Password::SET_SPECIAL_CHARACTERS
        );

        $db->executeStatement(sprintf(
            'CREATE ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($newWsRoleName),
        ));

        // grant workspace role to project user
        $db->executeStatement(sprintf(
            'GRANT %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($newWsRoleName),
            TeradataQuote::quoteSingleIdentifier($command->getProjectUserName()),
        ));

        // create workspace
        $db->executeStatement(sprintf(
            'CREATE USER %s FROM %s AS '
            . 'PERMANENT = %s, SPOOL = %s, '
            . 'PASSWORD = %s, DEFAULT DATABASE=%s, DEFAULT ROLE=%s;',
            TeradataQuote::quoteSingleIdentifier($newWsUserName),
            TeradataQuote::quoteSingleIdentifier($databaseName),
            $permSpace,
            $spoolSpace,
            TeradataQuote::quoteSingleIdentifier($newWsPassword),
            TeradataQuote::quoteSingleIdentifier($newWsUserName),
            TeradataQuote::quoteSingleIdentifier($newWsRoleName)
        ));

        // grant workspace role to workspace user
        $db->executeStatement(sprintf(
            'GRANT %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($newWsRoleName),
            TeradataQuote::quoteSingleIdentifier($newWsUserName)
        ));

        // grant read only role to ws role
        $db->executeStatement(sprintf(
            'GRANT %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($command->getProjectReadOnlyRoleName()),
            TeradataQuote::quoteSingleIdentifier($newWsRoleName)
        ));

        $db->close();

        return (new CreateWorkspaceResponse())
            ->setWorkspaceUserName($newWsUserName)
            ->setWorkspaceRoleName($newWsRoleName)
            ->setWorkspacePassword($newWsPassword)
            ->setWorkspaceObjectName($newWsUserName);
    }
}
