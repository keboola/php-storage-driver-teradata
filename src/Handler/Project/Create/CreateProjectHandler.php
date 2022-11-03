<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Project\Create;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\BackendSupportsInterface;
use Keboola\StorageDriver\Shared\Driver\MetaHelper;
use Keboola\StorageDriver\Shared\NameGenerator\NameGeneratorFactory;
use Keboola\StorageDriver\Shared\Utils\Password;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class CreateProjectHandler implements DriverCommandHandlerInterface
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
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateProjectCommand);

        $db = $this->manager->createSession($credentials);

        // root user is also root database
        $databaseName = $credentials->getPrincipal();
        $meta = $command->getMeta();
        if ($meta !== null) {
            // override root user and use other database as root
            $meta = $meta->unpack();
            assert($meta instanceof CreateProjectCommand\CreateProjectTeradataMeta);
            $databaseName = $meta->getRootDatabase() === '' ? $databaseName : $meta->getRootDatabase();
        }

        // allow override spaces for user
        /** @var CreateProjectCommand\CreateProjectTeradataMeta|null $meta */
        $meta = MetaHelper::getMetaRestricted($command, CreateProjectCommand\CreateProjectTeradataMeta::class);
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
        $newProjectRoleName = $nameGenerator->createRoleNameForProject($command->getProjectId());
        $newProjectReadOnlyRoleName = $nameGenerator->createReadOnlyRoleNameForProject($command->getProjectId());
        $newProjectUsername = $nameGenerator->createUserNameForProject($command->getProjectId());
        $newProjectPassword = Password::generate(
            30,
            Password::SET_LOWERCASE | Password::SET_UPPERCASE | Password::SET_NUMBER | Password::SET_SPECIAL_CHARACTERS
        );

        $db->executeStatement(sprintf(
            'CREATE ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($newProjectRoleName)
        ));

        $db->executeStatement(sprintf(
            'CREATE ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($newProjectReadOnlyRoleName)
        ));


        // grant project role to root user (@todo should we create also role for our root user and grant to it?)
        $db->executeStatement(sprintf(
            'GRANT %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($newProjectRoleName),
            TeradataQuote::quoteSingleIdentifier($credentials->getPrincipal()),
        ));

        $db->executeStatement(sprintf(
            'CREATE USER %s FROM %s AS '
            . 'PERMANENT = %s, SPOOL = %s, '
            . 'PASSWORD = %s, DEFAULT DATABASE=%s, DEFAULT ROLE=%s;',
            TeradataQuote::quoteSingleIdentifier($newProjectUsername),
            TeradataQuote::quoteSingleIdentifier($databaseName),
            $permSpace,
            $spoolSpace,
            TeradataQuote::quoteSingleIdentifier($newProjectPassword),
            TeradataQuote::quoteSingleIdentifier($newProjectUsername),
            TeradataQuote::quoteSingleIdentifier($newProjectRoleName)
        ));
        /* GRANT project RO role to project USER with admin option so project can GRANT it to workspace USER
         * we cannot GRANT it to project/ws ROLE, because TD has limit of nesting to 1
         * ROLE1 -> ROLE2 is ok, but adding ROLE2 -> ROLE3 fails
         */
        $db->executeStatement(sprintf(
            'GRANT %s TO %s WITH ADMIN OPTION;',
            TeradataQuote::quoteSingleIdentifier($newProjectReadOnlyRoleName),
            TeradataQuote::quoteSingleIdentifier($newProjectUsername)
        ));


        // grant create/drop user to project user
        // project user than can crete workspace
        // grant create/drop database to project role
        // this is needed to create buckets
        $db->executeStatement(sprintf(
            'GRANT CREATE USER, DROP USER, CREATE DATABASE, DROP DATABASE ON %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($newProjectUsername),
            TeradataQuote::quoteSingleIdentifier($newProjectUsername)
        ));

        // grant crete/drop role to project user
        // this is needed to create workspace
        $db->executeStatement(sprintf(
            'GRANT CREATE ROLE, DROP ROLE TO %s WITH GRANT OPTION;',
            TeradataQuote::quoteSingleIdentifier($newProjectUsername)
        ));

        // grant execute specific function to project user
        // this is needed to abort session for workspace
        $db->executeStatement(sprintf(
            'GRANT EXECUTE ON SPECIFIC FUNCTION SYSLIB.AbortSessions TO %s;',
            TeradataQuote::quoteSingleIdentifier($newProjectUsername)
        ));

        // grant project role to project user
        $db->executeStatement(sprintf(
            'GRANT %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($newProjectRoleName),
            TeradataQuote::quoteSingleIdentifier($newProjectUsername)
        ));

        $db->close();

        return (new CreateProjectResponse())
            ->setProjectUserName($newProjectUsername)
            ->setProjectRoleName($newProjectRoleName)
            ->setProjectReadOnlyRoleName($newProjectReadOnlyRoleName)
            ->setProjectPassword($newProjectPassword);
    }
}
