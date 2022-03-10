<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Project;

use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Project\Drop\DropProjectHandler;
use Keboola\StorageDriver\Teradata\TeradataAccessRight;

class CreateDropProjectTest extends BaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testCreateProject(): void
    {
        [$credentials, $response] = $this->createTestProject();

        $db = $this->getConnection($credentials);
        // test default database
        $this->assertSame(
            strtoupper($response->getProjectUserName()),
            $db->fetchOne('SELECT DATABASE;')
        );
        // test created users
        $this->assertTrue($this->isRoleExists($db, $response->getProjectRoleName()));
        $this->assertTrue($this->isRoleExists($db, $response->getProjectReadOnlyRoleName()));

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
            $this->getUserAccessRightForDatabase($db, $response->getProjectUserName(), $response->getProjectUserName())
        );

        $db->close();

        $handler = new DropProjectHandler($this->sessionManager);
        $command = (new DropProjectCommand())
            ->setProjectUserName($response->getProjectUserName())
            ->setProjectRoleName($response->getProjectRoleName())
            ->setReadOnlyRoleName($response->getProjectReadOnlyRoleName());

        $handler(
            $this->getCredentials(),
            $command,
            []
        );

        $db = $this->getConnection($this->getCredentials());

        $this->assertFalse($this->isUserExists($db, $response->getProjectUserName()));
        $this->assertFalse($this->isRoleExists($db, $response->getProjectRoleName()));
        $this->assertFalse($this->isRoleExists($db, $response->getProjectReadOnlyRoleName()));
    }
}
