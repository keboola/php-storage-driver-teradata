<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Project;

use Google\Protobuf\Any;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Exception\NoSpaceException;
use Keboola\StorageDriver\Teradata\Handler\Project\Create\CreateProjectHandler;
use Keboola\StorageDriver\Teradata\Handler\Project\Drop\DropProjectHandler;
use Keboola\StorageDriver\Teradata\TeradataAccessRight;
use Throwable;

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

    public function testCreateTooBigProject(): void
    {
        $handler = new CreateProjectHandler($this->sessionManager);
        $meta = new Any();
        $meta->pack(
            (new CreateProjectCommand\CreateProjectTeradataMeta())
                ->setRootDatabase($this->getRootDatabase())
                ->setPermSpace('10e15')
                ->setSpoolSpace('10e15')
        );
        $command = (new CreateProjectCommand())
            ->setProjectId($this->getProjectId())
            ->setStackPrefix($this->getStackPrefix())
            ->setMeta($meta);

        try {
            $handler(
                $this->getCredentials(),
                $command,
                []
            );
            $this->fail('should fail');
        } catch (Throwable $e) {
            $this->assertInstanceOf(NoSpaceException::class, $e);
            $this->assertEquals(ExceptionInterface::ERR_RESOURCE_FULL, $e->getCode());
            $this->assertEquals('Cannot create database because parent database is full.', $e->getMessage());
        }
    }
}
