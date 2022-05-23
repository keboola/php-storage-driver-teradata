<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Workspace\Drop\DropWorkspaceHandler;
use Keboola\StorageDriver\Teradata\TeradataAccessRight;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Throwable;

class CreateDropWorkspaceTest extends BaseCase
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

        $projectDb = $this->getConnection($this->projectCredentials);

        // check objects created
        $this->assertTrue($this->isUserExists($projectDb, $response->getWorkspaceUserName()));
        $this->assertTrue($this->isRoleExists($projectDb, $response->getWorkspaceRoleName()));
        $this->assertTrue($this->isDatabaseExists($projectDb, $response->getWorkspaceObjectName()));

        $db = $this->getConnection($credentials);

        // test defaults
        $this->assertSame(
            strtoupper($response->getWorkspaceRoleName()),
            $db->fetchOne('SELECT ROLE;')
        );
        $this->assertSame(
            strtoupper($response->getWorkspaceObjectName()),
            $db->fetchOne('SELECT DATABASE;')
        );

        // workspace user HAS ACCESS to workspace
        $this->assertEqualsArrays(
            [
                TeradataAccessRight::RIGHT_DROP_TABLE,
                TeradataAccessRight::RIGHT_EXECUTE,
                TeradataAccessRight::RIGHT_INSERT,
                TeradataAccessRight::RIGHT_DELETE,
                TeradataAccessRight::RIGHT_CREATE_VIEW,
                TeradataAccessRight::RIGHT_DROP_TRIGGER,
                TeradataAccessRight::RIGHT_DROP_FUNCTION,
                TeradataAccessRight::RIGHT_DROP_MACRO,
                TeradataAccessRight::RIGHT_DROP_PROCEDURE,
                TeradataAccessRight::RIGHT_CREATE_AUTHORIZATION,
                TeradataAccessRight::RIGHT_CREATE_MACRO,
                TeradataAccessRight::RIGHT_CREATE_TABLE,
                TeradataAccessRight::RIGHT_DUMP,
                TeradataAccessRight::RIGHT_UPDATE,
                TeradataAccessRight::RIGHT_CREATE_TRIGGER,
                TeradataAccessRight::RIGHT_CHECKPOINT,
                TeradataAccessRight::RIGHT_DROP_AUTHORIZATION,
                TeradataAccessRight::RIGHT_STATISTICS,
                TeradataAccessRight::RIGHT_RESTORE,
                TeradataAccessRight::RIGHT_RETRIEVE_OR_SELECT,
                TeradataAccessRight::RIGHT_DROP_VIEW,
            ],
            $this->getUserAccessRightForDatabase(
                $db,
                $response->getWorkspaceUserName(),
                $response->getWorkspaceObjectName()
            )
        );

        // workspace user HAS NOT ACCESS to project
        $this->assertEqualsArrays(
            [],
            $this->getUserAccessRightForDatabase(
                $db,
                $response->getWorkspaceUserName(),
                $this->projectResponse->getProjectUserName()
            )
        );

        // project user HAS ACCESS to workspace
        $this->assertEqualsArrays(
            [
                TeradataAccessRight::RIGHT_DROP_TABLE,
                TeradataAccessRight::RIGHT_EXECUTE,
                TeradataAccessRight::RIGHT_INSERT,
                TeradataAccessRight::RIGHT_DELETE,
                TeradataAccessRight::RIGHT_CREATE_VIEW,
                TeradataAccessRight::RIGHT_CREATE_DATABASE,
                TeradataAccessRight::RIGHT_DROP_DATABASE,
                TeradataAccessRight::RIGHT_DROP_TRIGGER,
                TeradataAccessRight::RIGHT_DROP_FUNCTION,
                TeradataAccessRight::RIGHT_DROP_MACRO,
                TeradataAccessRight::RIGHT_DROP_PROCEDURE,
                TeradataAccessRight::RIGHT_CREATE_AUTHORIZATION,
                TeradataAccessRight::RIGHT_CREATE_MACRO,
                TeradataAccessRight::RIGHT_CREATE_TABLE,
                TeradataAccessRight::RIGHT_DUMP,
                TeradataAccessRight::RIGHT_UPDATE,
                TeradataAccessRight::RIGHT_CREATE_USER,
                TeradataAccessRight::RIGHT_DROP_USER,
                TeradataAccessRight::RIGHT_CREATE_TRIGGER,
                TeradataAccessRight::RIGHT_CHECKPOINT,
                TeradataAccessRight::RIGHT_DROP_AUTHORIZATION,
                TeradataAccessRight::RIGHT_STATISTICS,
                TeradataAccessRight::RIGHT_RESTORE,
                TeradataAccessRight::RIGHT_RETRIEVE_OR_SELECT,
                TeradataAccessRight::RIGHT_DROP_VIEW,
            ],
            $this->getUserAccessRightForDatabase(
                $db,
                $this->projectResponse->getProjectUserName(),
                $response->getWorkspaceObjectName()
            )
        );

        // try to create table
        $db->executeStatement(sprintf(
            'CREATE TABLE %s."testTable" ("id" INTEGER);',
            TeradataQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        ));

        // try to create table in default database
        $db->executeStatement('CREATE TABLE "testTable2" ("id" INTEGER);');

        // try to create view
        $db->executeStatement(sprintf(
            'CREATE VIEW %s."testView" AS '
            . 'SELECT "id" FROM %s."testTable";',
            TeradataQuote::quoteSingleIdentifier($response->getWorkspaceObjectName()),
            TeradataQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        ));
        // try to create view  in default database
        $db->executeStatement('CREATE VIEW "testView2" AS SELECT "id" FROM "testTable2";');

        // try to drop view
        $db->executeStatement(sprintf(
            'DROP VIEW %s."testView";',
            TeradataQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        ));
        $db->executeStatement('DROP VIEW "testView2";');

        // try to drop table
        $db->executeStatement(sprintf(
            'DROP TABLE %s."testTable";',
            TeradataQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        ));
        $db->executeStatement('DROP TABLE "testTable2";');

        // dont close connection it should be ended in handler
        //$db->close();

        // DROP
        $handler = new DropWorkspaceHandler($this->sessionManager);
        $command = (new DropWorkspaceCommand())
            ->setWorkspaceUserName($response->getWorkspaceUserName())
            ->setWorkspaceRoleName($response->getWorkspaceRoleName())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName());

        $dropResponse = $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $this->assertNull($dropResponse);

        $projectDb = $this->getConnection($this->projectCredentials);
        $this->assertFalse($this->isUserExists($projectDb, $response->getWorkspaceUserName()));
        $this->assertFalse($this->isRoleExists($projectDb, $response->getWorkspaceRoleName()));
        $this->assertFalse($this->isDatabaseExists($projectDb, $response->getWorkspaceObjectName()));

        $projectDb->close();
    }

    public function testCreateDropCascadeWorkspace(): void
    {
        // CREATE
        [$credentials, $response] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);
        $this->assertInstanceOf(GenericBackendCredentials::class, $credentials);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $db = $this->getConnection($credentials);

        // create table
        $db->executeStatement(sprintf(
            'CREATE TABLE %s."testTable" ("id" INTEGER);',
            TeradataQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        ));

        $db->close();

        $projectDb = $this->getConnection($this->projectCredentials);

        // try to DROP - should fail, there is a table
        $handler = new DropWorkspaceHandler($this->sessionManager);
        $command = (new DropWorkspaceCommand())
            ->setWorkspaceUserName($response->getWorkspaceUserName())
            ->setWorkspaceRoleName($response->getWorkspaceRoleName())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName());
        try {
            $handler(
                $this->projectCredentials,
                $command,
                []
            );
            $this->fail('Should fail as workspace database contains table');
        } catch (Throwable $e) {
            $this->assertStringContainsString(
                'Cannot DROP databases with tables, journal tables, views, macros, or zones.',
                $e->getMessage()
            );
        }
        $this->assertTrue($this->isDatabaseExists($projectDb, $response->getWorkspaceObjectName()));
        $this->assertTrue($this->isUserExists($projectDb, $response->getWorkspaceUserName()));
        $this->assertTrue($this->isRoleExists($projectDb, $response->getWorkspaceRoleName()));

        // try to DROP - should not fail and database will be deleted
        $command->setIsCascade(true);
        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $projectDb = $this->getConnection($this->projectCredentials);
        $this->assertFalse($this->isDatabaseExists($projectDb, $response->getWorkspaceObjectName()));
        $this->assertFalse($this->isUserExists($projectDb, $response->getWorkspaceUserName()));
        $this->assertFalse($this->isRoleExists($projectDb, $response->getWorkspaceRoleName()));

        $projectDb->close();
    }
}
