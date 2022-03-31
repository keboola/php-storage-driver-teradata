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
    public const DEFAULT_PERM_SPACE_SIZE = 1e9; // 1GB
    public const DEFAULT_SPOOL_SPACE_SIZE = 1e9; // 1GB

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
        assert(!empty($command->getStackPrefix()), 'CreateWorkspaceCommand.stackPrefix is required');
        assert(!empty($command->getWorkspaceId()), 'CreateWorkspaceCommand.workspaceId is required');
        assert(!empty($command->getProjectUserName()), 'CreateWorkspaceCommand.projectUserName is required');
        assert(!empty($command->getProjectRoleName()), 'CreateWorkspaceCommand.projectRoleName is required');
        assert(
            !empty($command->getProjectReadOnlyRoleName()),
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

        // grant workspace role to root user
        $db->executeStatement(sprintf(
            'GRANT %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($newWsRoleName),
            TeradataQuote::quoteSingleIdentifier($credentials->getPrincipal()),
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

        // grant select to project read only role
        $db->executeStatement(sprintf(
            'GRANT SELECT ON %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($newWsUserName),
            TeradataQuote::quoteSingleIdentifier($command->getProjectReadOnlyRoleName()),
        ));

        $db->close();

        return (new CreateWorkspaceResponse())
            ->setWorkspaceUserName($newWsUserName)
            ->setWorkspaceRoleName($newWsRoleName)
            ->setWorkspacePassword($newWsPassword)
            ->setWorkspaceObjectName($newWsUserName);
    }
}