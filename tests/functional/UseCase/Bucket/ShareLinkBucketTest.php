<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Share\ShareBucketHandler;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Throwable;

class ShareLinkBucketTest extends BaseCase
{
    protected GenericBackendCredentials $project1Credentials;
    protected CreateProjectResponse $project1Response;

    protected GenericBackendCredentials $project2Credentials;
    protected CreateProjectResponse $project2Response;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$credentials1, $response1] = $this->createTestProject();
        $this->projectSuffix = '_second';
        $this->cleanTestProject();
        [$credentials2, $response2] = $this->createTestProject();

        // project1 shares bucket
        $this->project1Credentials = $credentials1;
        $this->project1Response = $response1;

        // project2 checks the access
        $this->project2Credentials = $credentials2;
        $this->project2Response = $response2;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->projectSuffix = '';
        $this->cleanTestProject();
        $this->projectSuffix = '_second';
        $this->cleanTestProject();
    }

    public function testShareBucket(): void
    {
        [$bucketResponse, $projectConnection] = $this->createTestBucket(
            $this->project1Credentials,
            $this->project1Response
        );

        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();

        // $projectConnection doesnt have set bucket DB yet
        $projectConnection->executeStatement(sprintf(
            'SET SESSION DATABASE %s;',
            TeradataQuote::quoteSingleIdentifier($bucketDatabaseName)
        ));
        $shareRoleName = $bucketDatabaseName . '_SHARE';
        // cleaning of the share role
        $this->dropRole($projectConnection, $shareRoleName);

        $projectConnection->executeQuery('CREATE TABLE TESTTABLE_BEFORE (ID INT)');
        $projectConnection->executeQuery('INSERT INTO TESTTABLE_BEFORE (1)');

        $project2Connection = $this->getConnection($this->project2Credentials);

        // check that the Project2 cannot access the table yet
        try {
            $project2Connection->fetchAllAssociative(
                sprintf(
                    'SELECT * FROM %s.%s',
                    TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                    TeradataQuote::quoteSingleIdentifier('TESTTABLE_BEFORE')
                )
            );
            $this->fail('role is not graneted yet');
        } catch (Throwable $e) {
            $this->assertStringContainsString('The user does not have SELECT access to', $e->getMessage());
        }

        // share the bucket
        $handler = new ShareBucketHandler($this->sessionManager);
        $command = (new ShareBucketCommand())
            ->setBucketObjectName($bucketDatabaseName)
            ->setProjectReadOnlyRoleName($this->project1Response->getProjectReadOnlyRoleName())
            ->setBucketShareRoleName($shareRoleName);

        $handler(
            $this->project1Credentials,
            $command,
            []
        );

        // let's simulate bucket link -> grant SHARE_ROLE to project2 user and try to access the DB.
        $project1Connection = $this->getConnection($this->project1Credentials);
        $project1Connection->executeQuery(
            sprintf(
                'GRANT %s TO %s',
                TeradataQuote::quoteSingleIdentifier($shareRoleName),
                TeradataQuote::quoteSingleIdentifier($this->project2Credentials->getPrincipal())
            )
        );

        // check that there is no need to re-share or whatever
        $projectConnection->executeQuery('CREATE TABLE TESTTABLE_AFTER (ID INT)');
        $projectConnection->executeQuery('INSERT INTO TESTTABLE_AFTER (1)');

        $dataBefore = $project2Connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier('TESTTABLE_BEFORE')
            )
        );
        $dataAfter = $project2Connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier('TESTTABLE_AFTER')
            )
        );

        $this->assertEquals([['ID' => '1']], $dataAfter);
        $this->assertEquals($dataBefore, $dataAfter);
    }
}
