<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Project\Create;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\ConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class CreateProjectHandler implements DriverCommandHandlerInterface
{
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

        $db->executeStatement(sprintf(
            'CREATE ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($command->getProjectRole())
        ));

        $db->executeStatement(sprintf(
            'CREATE ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($command->getReadOnlyRoleName())
        ));

        $db->executeStatement(sprintf(
            'GRANT %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($command->getReadOnlyRoleName()),
            TeradataQuote::quoteSingleIdentifier($command->getProjectRole())
        ));

        $db->executeStatement(sprintf(
            'CREATE USER %s FROM %s AS '
            .'PERMANENT = 60000000, SPOOL = 120000000, '
            .'PASSWORD = %s, DEFAULT DATABASE=%s, DEFAULT ROLE=%s;',
            TeradataQuote::quoteSingleIdentifier($command->getProjectUser()),
            TeradataQuote::quoteSingleIdentifier($databaseName),
            $command->getProjectPassword(),
            TeradataQuote::quoteSingleIdentifier($command->getProjectUser()),
            TeradataQuote::quoteSingleIdentifier($command->getProjectRole())
        ));

        $db->executeStatement(sprintf(
            'GRANT %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($command->getProjectRole()),
            TeradataQuote::quoteSingleIdentifier($command->getProjectUser())
        ));
    }
}
