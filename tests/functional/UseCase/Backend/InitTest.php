<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Backend;

use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Backend\Init\InitBackendHandler;

class InitTest extends BaseCase
{
    /**
     * @doesNotPerformAssertions
     */
    public function testInitBackend(): void
    {
        $handler = new InitBackendHandler();
        $command = new InitBackendCommand();
        $handler(
            $this->getCredentials(),
            $command,
            []
        );
    }
}
