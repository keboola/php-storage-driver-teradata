<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleCommand;
use Keboola\StorageDriver\Command\Bucket\RevokeBucketAccessFromReadOnlyRoleCommand;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Create\GrantBucketAccessToReadOnlyRoleHandler;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Drop\RevokeBucketAccessFromReadOnlyRoleHandler;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Throwable;

class GrantRevokeBucketAccessToReadOnlyRoleTest extends BaseCase
{
    protected GenericBackendCredentials $mainProjectCredentials;
    protected CreateProjectResponse $mainProjectResponse;

    // external bucket (=external DB) will be represented by a bucket in another project (not shared/linked)
    protected GenericBackendCredentials $externalProjectCredentials;
    protected CreateProjectResponse $externalProjectResponse;

    private const PROJ_SUFFIX = '_external';

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$credentials1, $response1] = $this->createTestProject();
        $this->projectSuffix = self::PROJ_SUFFIX;
        $this->cleanTestProject();
        [$credentials2, $response2] = $this->createTestProject();

        // project1 shares bucket
        $this->mainProjectCredentials = $credentials1;
        $this->mainProjectResponse = $response1;

        // project2 checks the access
        $this->externalProjectCredentials = $credentials2;
        $this->externalProjectResponse = $response2;

        $this->cleanTestWorkspace();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->projectSuffix = '';
        $this->cleanTestProject();
        $this->projectSuffix = self::PROJ_SUFFIX;
        $this->cleanTestProject();
    }

    public function testWSCanAccessExternalBucketAfterGrantingAndRevokingToRO(): void
    {
        // create a DB and table in EXT project.
        [$externalBucketResponse, $externalProjectConnection] = $this->createTestBucket(
            $this->externalProjectCredentials,
            $this->externalProjectResponse
        );

        $externalBucketDatabaseName = $externalBucketResponse->getCreateBucketObjectName();

        // $projectConnection doesnt have set bucket DB yet
        $externalProjectConnection->executeStatement(sprintf(
            'SET SESSION DATABASE %s;',
            TeradataQuote::quoteSingleIdentifier($externalBucketDatabaseName)
        ));

        $externalProjectConnection->executeQuery('CREATE TABLE TESTTABLE (ID INT)');
        $externalProjectConnection->executeQuery('INSERT INTO TESTTABLE (1)');

        [$wsCredentials, $wsRespones] = $this->createTestWorkspace(
            $this->mainProjectCredentials,
            $this->mainProjectResponse
        );

        $wsConnection = $this->getConnection($wsCredentials);

        // check that the Project2 cannot access the table yet
        try {
            $wsConnection->fetchAllAssociative(
                sprintf(
                    'SELECT * FROM %s.%s',
                    TeradataQuote::quoteSingleIdentifier($externalBucketDatabaseName),
                    TeradataQuote::quoteSingleIdentifier('TESTTABLE')
                )
            );
            $this->fail('Should fail. Bucket is not registered as external yet');
        } catch (Throwable $e) {
            $this->assertStringContainsString('The user does not have SELECT access to', $e->getMessage());
        }

        // granting SELECT to PROJ user - this query should run owner of the external DB to bring it as ext bucket
        $externalProjectConnection->executeQuery(
            sprintf(
                'GRANT SELECT ON %s TO %s WITH GRANT OPTION;',
                TeradataQuote::quoteSingleIdentifier($externalBucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier($this->mainProjectCredentials->getPrincipal())
            )
        );

        // GRANT SELECT on ext bucket to RO
        $handler = new GrantBucketAccessToReadOnlyRoleHandler($this->sessionManager);
        $command = (new GrantBucketAccessToReadOnlyRoleCommand())
            ->setPath([$externalBucketDatabaseName])
            ->setProjectReadOnlyRoleName($this->mainProjectResponse->getProjectReadOnlyRoleName());

        $handler(
            $this->mainProjectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        // WS should be able to select now
        $data = $wsConnection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                TeradataQuote::quoteSingleIdentifier($externalBucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier('TESTTABLE')
            )
        );
        $this->assertEquals([['ID' => '1']], $data);

        // REVOKE SELECT on ext bucket from RO
        $handler = new RevokeBucketAccessFromReadOnlyRoleHandler($this->sessionManager);
        $command = (new RevokeBucketAccessFromReadOnlyRoleCommand())
            ->setBucketObjectName($externalBucketDatabaseName)
            ->setProjectReadOnlyRoleName($this->mainProjectResponse->getProjectReadOnlyRoleName());

        $handler(
            $this->mainProjectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        // check that the Project2 cannot access the table
        try {
            $wsConnection->fetchAllAssociative(
                sprintf(
                    'SELECT * FROM %s.%s',
                    TeradataQuote::quoteSingleIdentifier($externalBucketDatabaseName),
                    TeradataQuote::quoteSingleIdentifier('TESTTABLE')
                )
            );
            $this->fail('Should fail. Bucket is no longer registered as external');
        } catch (Throwable $e) {
            $this->assertStringContainsString('The user does not have SELECT access to', $e->getMessage());
        }
    }
}
