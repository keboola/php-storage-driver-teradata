<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Info;

use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\ObjectNotFoundException;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Teradata\Handler\Info\ObjectInfoHandler;

class ObjectInfoErrorTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    private CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();
        // create project
        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;
        $this->projectResponse = $projectResponse;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testInfoDatabaseNotExists(): void
    {
        $handler = new ObjectInfoHandler($this->sessionManager);
        $command = new ObjectInfoCommand();
        // expect database
        $command->setExpectedObjectType(ObjectType::DATABASE);
        $command->setPath(ProtobufHelper::arrayToRepeatedString(['iAmNotExist']));
        $this->expectException(ObjectNotFoundException::class);
        $this->expectExceptionMessage('Object "iAmNotExist" not found.');
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );
    }

    public function testInfoSchemaNotExists(): void
    {
        $handler = new ObjectInfoHandler($this->sessionManager);
        $command = new ObjectInfoCommand();
        // expect database
        $command->setExpectedObjectType(ObjectType::SCHEMA);
        $command->setPath(ProtobufHelper::arrayToRepeatedString(['iAmNotExist']));
        $this->expectException(ObjectNotFoundException::class);
        $this->expectExceptionMessage('Object "iAmNotExist" not found.');
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );
    }

    public function testInfoTableDatabaseNotExists(): void
    {
        $handler = new ObjectInfoHandler($this->sessionManager);
        $command = new ObjectInfoCommand();
        // expect database
        $command->setExpectedObjectType(ObjectType::TABLE);
        $command->setPath(ProtobufHelper::arrayToRepeatedString(['databaseNotExists', 'iAmNotExist']));
        $this->expectException(ObjectNotFoundException::class);
        $this->expectExceptionMessage('Object "iAmNotExist" not found.');
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );
    }

    public function testInfoTableNotExists(): void
    {
        $handler = new ObjectInfoHandler($this->sessionManager);
        $command = new ObjectInfoCommand();
        // expect database
        $command->setExpectedObjectType(ObjectType::TABLE);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([
            $this->projectResponse->getProjectUserName(),
            'iAmNotExist',
        ]));
        $this->expectException(ObjectNotFoundException::class);
        $this->expectExceptionMessage('Object "iAmNotExist" not found.');
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );
    }

    public function testInfoViewNotExists(): void
    {
        $this->markTestSkipped('View info is TODO');
        //$handler = new ObjectInfoHandler($this->sessionManager);
        //$command = new ObjectInfoCommand();
        //// expect database
        //$command->setExpectedObjectType(ObjectType::VIEW);
        //$command->setPath(ProtobufHelper::arrayToRepeatedString(['databaseNotExists', 'iAmNotExist']));
        //$this->expectException(ObjectNotFoundException::class);
        //$this->expectExceptionMessage('Object "iAmNotExist" not found.');
        //$handler(
        //    $this->projectCredentials,
        //    $command,
        //    []
        //);
    }
}
