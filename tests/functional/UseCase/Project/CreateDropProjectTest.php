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
        $this->createTestProject();

        $credentials = $this->getTestProjectCredentials();
        $db = $this->getConnection($credentials);
        // test default database
        $this->assertSame(
            strtoupper($this->getProjectUser()),
            $db->fetchOne('SELECT DATABASE;')
        );
        // test created users
        $this->assertTrue($this->isRoleExists($db, $this->getProjectRole()));
        $this->assertTrue($this->isRoleExists($db, $this->getProjectReadOnlyRole()));

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
            $this->getUserAccessRightForDatabase($db, $this->getProjectUser(), $this->getProjectUser())
        );

        $db->close();

        $handler = new DropProjectHandler();
        $command = (new DropProjectCommand())
            ->setProjectUser($this->getProjectUser())
            ->setProjectRole($this->getProjectRole())
            ->setReadOnlyRoleName($this->getProjectReadOnlyRole());

        $handler(
            $this->getCredentials(),
            $command,
            []
        );

        $db = $this->getConnection($this->getCredentials());

        $this->assertFalse($this->isUserExists($db, $this->getProjectUser()));
        $this->assertFalse($this->isRoleExists($db, $this->getProjectRole()));
        $this->assertFalse($this->isRoleExists($db, $this->getProjectReadOnlyRole()));
    }
}
