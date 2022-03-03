<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests;

use Keboola\StorageDriver\Command\Backend\RemoveBackendCommand;
use Keboola\StorageDriver\Shared\Driver\Exception\CommandNotSupportedException;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\GeneratedTests\CustomMessage;
use Keboola\StorageDriver\Teradata\TeradataDriverClient;
use PHPUnit\Framework\TestCase;

class TeradataDriverClientTest extends TestCase
{
    public function testNotSupportedCommand(): void
    {
        $client = new TeradataDriverClient();

        $this->expectException(CommandNotSupportedException::class);
        $client->runCommand(
            $this->createMock(GenericBackendCredentials::class),
            new CustomMessage(),
            []
        );
    }

    public function testSupportedCommand(): void
    {
        $this->expectNotToPerformAssertions();

        $client = new TeradataDriverClient();

        $client->runCommand(
            $this->createMock(GenericBackendCredentials::class),
            $this->createMock(RemoveBackendCommand::class),
            []
        );
    }
}
