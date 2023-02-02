<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Project\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\DbUtils;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class DropProjectHandler implements DriverCommandHandlerInterface
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
        assert($command instanceof DropProjectCommand);

        $db = $this->manager->createSession($credentials);

        $db->executeStatement(sprintf(
            'DROP ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($command->getProjectRoleName())
        ));

        $db->executeStatement(sprintf(
            'DROP ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($command->getReadOnlyRoleName())
        ));

        DbUtils::cleanUserOrDatabase(
            $db,
            $command->getProjectUserName(),
            $credentials->getPrincipal(),
        );

        $db->close();
        return null;
    }
}
