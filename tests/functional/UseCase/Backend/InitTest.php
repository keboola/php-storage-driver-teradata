<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Backend;

use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Backend\InitBackendResponse;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Backend\Init\InitBackendHandler;

class InitTest extends BaseCase
{
    public function testInitBackend(): void
    {
        $handler = new InitBackendHandler($this->sessionManager);
        $command = new InitBackendCommand();
        $response = $handler(
            $this->getCredentials(),
            $command,
            []
        );
        $this->assertInstanceOf(InitBackendResponse::class, $response);
    }
}
