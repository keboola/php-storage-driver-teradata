<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Backend\Remove;

use Keboola\StorageDriver\Contract\Credentials\CredentialsInterface;
use Keboola\StorageDriver\Contract\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Contract\Driver\Command\Backend\Remove\RemoveBackendCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandInterface;

final class RemoveBackendHandler implements DriverCommandHandlerInterface
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
        assert($command instanceof RemoveBackendCommand);
    }
}
