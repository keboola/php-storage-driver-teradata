<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Workspace\ClearWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Workspace\Clear\ClearWorkspaceHandler;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Throwable;

class ClearWorkspaceTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestWorkspace();
        $this->cleanTestProject();

        [$credentials, $response] = $this->createTestProject();
        $this->projectCredentials = $credentials;
        $this->projectResponse = $response;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestWorkspace();
        $this->cleanTestProject();
    }

    public function testClearWorkspace(): void
    {
        // CREATE
        [$credentials, $response] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);
        $this->assertInstanceOf(GenericBackendCredentials::class, $credentials);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $db = $this->getConnection($credentials);

        // create tables
        $db->executeStatement(sprintf(
            'CREATE TABLE %s."testTable" ("id" INTEGER);',
            TeradataQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        ));
        $db->executeStatement(sprintf(
            'CREATE TABLE %s."testTable2" ("id" INTEGER);',
            TeradataQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        ));

        $db->close();

        // CLEAR with BAD OBJECT NAME
        $handler = new ClearWorkspaceHandler($this->sessionManager);
        $command = (new ClearWorkspaceCommand())
            ->setWorkspaceObjectName('objectNotExists');

        try {
            $handler(
                $this->projectCredentials,
                $command,
                []
            );
            $this->fail('Should fail');
        } catch (Throwable $e) {
            $this->assertStringContainsString(
                "Database 'objectNotExists' does not exist.",
                $e->getMessage()
            );
        }

        // CLEAR with BAD OBJECT NAME and IGNORE ERRORS
        $handler = new ClearWorkspaceHandler($this->sessionManager);
        $command = (new ClearWorkspaceCommand())
            ->setWorkspaceObjectName('objectNotExists')
            ->setIgnoreErrors(true);

        $clearResponse = $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $this->assertNull($clearResponse);

        $projectDb = $this->getConnection($this->projectCredentials);
        $this->assertTrue($this->isTableExists($projectDb, $response->getWorkspaceObjectName(), 'testTable'));
        $this->assertTrue($this->isTableExists($projectDb, $response->getWorkspaceObjectName(), 'testTable2'));
        $this->assertTrue($this->isDatabaseExists($projectDb, $response->getWorkspaceObjectName()));
        $this->assertTrue($this->isUserExists($projectDb, $response->getWorkspaceUserName()));
        $this->assertTrue($this->isRoleExists($projectDb, $response->getWorkspaceRoleName()));

        // CLEAR
        $handler = new ClearWorkspaceHandler($this->sessionManager);
        $command = (new ClearWorkspaceCommand())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName());

        $clearResponse = $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $this->assertNull($clearResponse);

        $projectDb = $this->getConnection($this->projectCredentials);
        $this->assertFalse($this->isTableExists($projectDb, $response->getWorkspaceObjectName(), 'testTable'));
        $this->assertFalse($this->isTableExists($projectDb, $response->getWorkspaceObjectName(), 'testTable2'));
        $this->assertTrue($this->isDatabaseExists($projectDb, $response->getWorkspaceObjectName()));
        $this->assertTrue($this->isUserExists($projectDb, $response->getWorkspaceUserName()));
        $this->assertTrue($this->isRoleExists($projectDb, $response->getWorkspaceRoleName()));

        $projectDb->close();
    }
}
