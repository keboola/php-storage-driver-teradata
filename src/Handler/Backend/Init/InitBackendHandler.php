<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Backend\Init;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\ConnectionFactory;

final class InitBackendHandler implements DriverCommandHandlerInterface
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
        assert($command instanceof InitBackendCommand);

        $db = ConnectionFactory::getConnection($credentials);

        $db->fetchOne('SELECT 1');

        // @todo test if user can create user/role/database maybe something other?
    }
}
