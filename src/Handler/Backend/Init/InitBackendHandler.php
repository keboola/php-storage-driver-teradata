<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Backend\Init;

use Keboola\StorageDriver\Contract\Credentials\CredentialsInterface;
use Keboola\StorageDriver\Contract\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Contract\Driver\Command\Backend\Init\InitBackendCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandInterface;
use Keboola\StorageDriver\Teradata\ConnectionFactory;

final class InitBackendHandler implements DriverCommandHandlerInterface
{
    /**
     * @inheritDoc
     */
    public function __invoke(
        CredentialsInterface $credentials,
        DriverCommandInterface $command,
        array $features
    ) {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof InitBackendCommand);

        $db = ConnectionFactory::getConnection($credentials);

        $db->fetchOne('SELECT 1');
    }
}
