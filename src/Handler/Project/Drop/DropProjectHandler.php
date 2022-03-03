<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Project\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\ConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class DropProjectHandler implements DriverCommandHandlerInterface
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
        assert($command instanceof DropProjectCommand);

        $db = ConnectionFactory::getConnection($credentials);

        $db->executeStatement(sprintf(
            'DROP ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($command->getProjectRoleName())
        ));

        $db->executeStatement(sprintf(
            'DROP ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($command->getReadOnlyRoleName())
        ));

        $db->executeStatement(sprintf(
            'DROP USER %s;',
            TeradataQuote::quoteSingleIdentifier($command->getProjectUserName())
        ));

        $db->close();
    }
}
