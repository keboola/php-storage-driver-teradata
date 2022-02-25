<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Project\Create;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Contract\Driver\MetaHelper;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\ConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class CreateProjectHandler implements DriverCommandHandlerInterface
{
    public const DEFAULT_PERM_SPACE_SIZE = 60000000;
    public const DEFAULT_SPOOL_SPACE_SIZE = 120000000;

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ) {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateProjectCommand);

        $db = ConnectionFactory::getConnection($credentials);

        // root user is also root database
        $databaseName = $credentials->getPrincipal();
        $meta = $command->getMeta();
        if ($meta !== null) {
            // override root user and use other database as root
            $meta = $meta->unpack();
            assert($meta instanceof CreateProjectCommand\CreateProjectTeradataMeta);
            $databaseName = $meta->getRootDatabase();
        }

        // allow override spaces for user
        /** @var CreateProjectCommand\CreateProjectTeradataMeta|null $meta */
        $meta = MetaHelper::getMetaRestricted($command, CreateProjectCommand\CreateProjectTeradataMeta::class);
        $permSpace = self::DEFAULT_PERM_SPACE_SIZE;
        $spoolSpace = self::DEFAULT_SPOOL_SPACE_SIZE;
        if ($meta !== null) {
            $permSpace = $meta->getPermSpace() !== '' ? $meta->getPermSpace() : $permSpace;
            $spoolSpace = $meta->getSpoolSpace() !== '' ? $meta->getSpoolSpace() : $permSpace;
        }

        $db->executeStatement(sprintf(
            'CREATE ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($command->getProjectRole())
        ));

        $db->executeStatement(sprintf(
            'CREATE ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($command->getReadOnlyRoleName())
        ));

        // grant project read only role to project role
        $db->executeStatement(sprintf(
            'GRANT %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($command->getReadOnlyRoleName()),
            TeradataQuote::quoteSingleIdentifier($command->getProjectRole())
        ));

        // grant project role to root user (@todo should we create also role for our root user and grant to it?)
        $db->executeStatement(sprintf(
            'GRANT %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($command->getProjectRole()),
            TeradataQuote::quoteSingleIdentifier($credentials->getPrincipal()),
        ));

        $db->executeStatement(sprintf(
            'CREATE USER %s FROM %s AS '
            . 'PERMANENT = %s, SPOOL = %s, '
            . 'PASSWORD = %s, DEFAULT DATABASE=%s, DEFAULT ROLE=%s;',
            TeradataQuote::quoteSingleIdentifier($command->getProjectUser()),
            TeradataQuote::quoteSingleIdentifier($databaseName),
            $permSpace,
            $spoolSpace,
            $command->getProjectPassword(),
            TeradataQuote::quoteSingleIdentifier($command->getProjectUser()),
            TeradataQuote::quoteSingleIdentifier($command->getProjectRole())
        ));

        // grant create/drop user to project user
        // project user than can crete workspace
        // grant create/drop database to project role
        // this is needed to create buckets
        $db->executeStatement(sprintf(
            'GRANT CREATE USER, DROP USER, CREATE DATABASE, DROP DATABASE ON %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($command->getProjectUser()),
            TeradataQuote::quoteSingleIdentifier($command->getProjectUser())
        ));

        // grant project role to project user
        $db->executeStatement(sprintf(
            'GRANT %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($command->getProjectRole()),
            TeradataQuote::quoteSingleIdentifier($command->getProjectUser())
        ));

        $db->close();
    }
}
