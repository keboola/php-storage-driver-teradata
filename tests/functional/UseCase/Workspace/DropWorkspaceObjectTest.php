<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceObjectCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Workspace\DropObject\DropWorkspaceObjectHandler;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Throwable;

class DropWorkspaceObjectTest extends BaseCase
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

    public function testCreateDropWorkspace(): void
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

        // create view
        $db->executeStatement(sprintf(
            'CREATE VIEW %s."testView" AS '
            . 'SELECT "id" FROM %s."testTable";',
            TeradataQuote::quoteSingleIdentifier($response->getWorkspaceObjectName()),
            TeradataQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        ));

        $db->close();

        // DROP with BAD TABLE NAME
        $handler = new DropWorkspaceObjectHandler($this->sessionManager);
        $command = (new DropWorkspaceObjectCommand())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName())
            ->setObjectNameToDrop('objectNotExists');

        try {
            $handler(
                $credentials,
                $command,
                []
            );
            $this->fail('Should fail');
        } catch (Throwable $e) {
            $this->assertStringContainsString(
                sprintf(
                    "Object '%s.%s' does not exist.",
                    $response->getWorkspaceObjectName(),
                    'objectNotExists'
                ),
                $e->getMessage()
            );
        }

        // DROP with BAD TABLE NAME with IGNORE
        $handler = new DropWorkspaceObjectHandler($this->sessionManager);
        $command = (new DropWorkspaceObjectCommand())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName())
            ->setObjectNameToDrop('objectNotExists')
            ->setIgnoreIfNotExists(true);

        $dropResponse = $handler(
            $credentials,
            $command,
            []
        );
        $this->assertNull($dropResponse);

        $db = $this->getConnection($credentials);
        $this->assertTrue($this->isTableExists($db, $response->getWorkspaceObjectName(), 'testTable'));
        $this->assertTrue($this->isTableExists($db, $response->getWorkspaceObjectName(), 'testTable2'));

        // DROP table
        $handler = new DropWorkspaceObjectHandler($this->sessionManager);
        $command = (new DropWorkspaceObjectCommand())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName())
            ->setObjectNameToDrop('testTable2');

        $dropResponse = $handler(
            $credentials,
            $command,
            []
        );
        $this->assertNull($dropResponse);

        $db = $this->getConnection($credentials);
        $this->assertTrue($this->isTableExists($db, $response->getWorkspaceObjectName(), 'testTable'));
        $this->assertFalse($this->isTableExists($db, $response->getWorkspaceObjectName(), 'testTable2'));

        // DROP table used in view
        $handler = new DropWorkspaceObjectHandler($this->sessionManager);
        $command = (new DropWorkspaceObjectCommand())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName())
            ->setObjectNameToDrop('testTable');

        $dropResponse = $handler(
            $credentials,
            $command,
            []
        );
        $this->assertNull($dropResponse);

        $db = $this->getConnection($credentials);
        $this->assertFalse($this->isTableExists($db, $response->getWorkspaceObjectName(), 'testTable'));
        $this->assertFalse($this->isTableExists($db, $response->getWorkspaceObjectName(), 'testTable2'));

        $db->close();
    }
}
