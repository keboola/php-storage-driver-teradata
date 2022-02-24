<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Backend;

use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Backend\RemoveBackendCommand;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Backend\Init\InitBackendHandler;
use Keboola\StorageDriver\Teradata\Handler\Backend\Remove\RemoveBackendHandler;

class RemoveTest extends BaseCase
{
    /**
     * @doesNotPerformAssertions
     */
    public function testInitBackend(): void
    {
        $handler = new RemoveBackendHandler();
        $command = new RemoveBackendCommand();
        $handler(
            $this->getCredentials(),
            $command,
            []
        );
    }
}
