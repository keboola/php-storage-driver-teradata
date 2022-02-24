<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Project;

use Keboola\StorageDriver\FunctionalTests\BaseCase;

class CreateProjectTest extends BaseCase
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
    }
}
