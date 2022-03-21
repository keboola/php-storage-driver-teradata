<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Workspace\Drop\DropWorkspaceHandler;

class CreateDropWorkspaceTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();
        [$credentials, $response] = $this->createTestProject();
        $this->projectCredentials = $credentials;
        $this->projectResponse = $response;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testCreateDropWorkspace(): void
    {
        [$response, $db] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);

        $handler = new DropWorkspaceHandler($this->sessionManager);
        $command = (new DropWorkspaceCommand())
            ->setWorkspaceUserName($response->getWorkspaceUserName())
            ->setWorkspaceRoleName($response->getWorkspaceRoleName())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName());

        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $this->assertFalse($this->isDatabaseExists($db, $response->getWorkspaceObjectName()));

        $db->close();
    }
}
