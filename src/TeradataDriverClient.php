<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Keboola\StorageDriver\Contract\Credentials\CredentialsInterface;
use Keboola\StorageDriver\Contract\Driver\ClientInterface;
use Keboola\StorageDriver\Contract\Driver\Command\Backend\Init\InitBackendCommand;
use Keboola\StorageDriver\Contract\Driver\Command\Backend\Remove\RemoveBackendCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandInterface;
use Keboola\StorageDriver\Contract\Driver\Exception\CommandNotSupportedException;
use Keboola\StorageDriver\Teradata\Handler\Backend\Init\InitBackendHandler;
use Keboola\StorageDriver\Teradata\Handler\Backend\Remove\RemoveBackendHandler;

class TeradataDriverClient implements ClientInterface
{
    /**
     * @inheritDoc
     */
    public function runCommand(
        string $backend,
        CredentialsInterface $credentials,
        DriverCommandInterface $command,
        array $features
    ) {
        $handler = $this->getHandler($command);
        return $handler(
            $credentials,
            $command,
            $features
        );
    }

    private function getHandler(DriverCommandInterface $command): DriverCommandHandlerInterface
    {
        switch (true) {
            case $command instanceof InitBackendCommand:
                return new InitBackendHandler();
            case $command instanceof RemoveBackendCommand:
                return new RemoveBackendHandler();
        }

        throw new CommandNotSupportedException($command::getCommandName());
    }
}
