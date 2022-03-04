<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Project;

use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Project\Drop\DropProjectHandler;

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
        [$credentials,$response] = $this->createTestProject();

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
                'DG',
                'DT',
                'E ',
                'I ',
                'D ',
                'CV',
                'CD',
                'DD',
                'DF',
                'DM',
                'PD',
                'CA',
                'CM',
                'CT',
                'DP',
                'U ',
                'CU',
                'DU',
                'CG',
                'CP',
                'DA',
                'ST',
                'RS',
                'R ',
                'DV',
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
