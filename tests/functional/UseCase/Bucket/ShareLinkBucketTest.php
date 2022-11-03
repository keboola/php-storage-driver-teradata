<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Link\LinkBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Share\ShareBucketHandler;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Throwable;

class ShareLinkBucketTest extends BaseCase
{
    protected GenericBackendCredentials $sourceProjectCredentials;
    protected CreateProjectResponse $sourceProjectResponse;

    protected GenericBackendCredentials $targetProjectCredentials;
    protected CreateProjectResponse $targetProjectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$credentials1, $response1] = $this->createTestProject();
        $this->projectSuffix = '_second';
        $this->cleanTestProject();
        [$credentials2, $response2] = $this->createTestProject();

        // project1 shares bucket
        $this->sourceProjectCredentials = $credentials1;
        $this->sourceProjectResponse = $response1;

        // project2 checks the access
        $this->targetProjectCredentials = $credentials2;
        $this->targetProjectResponse = $response2;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->projectSuffix = '';
        $this->cleanTestProject();
        $this->projectSuffix = '_second';
        $this->cleanTestProject();
    }

    public function testShareAndLinkBucket(): void
    {
        [$bucketResponse, $sourceProjectConnection] = $this->createTestBucket(
            $this->sourceProjectCredentials,
            $this->sourceProjectResponse
        );

        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();

        // $projectConnection doesnt have set bucket DB yet
        $sourceProjectConnection->executeStatement(sprintf(
            'SET SESSION DATABASE %s;',
            TeradataQuote::quoteSingleIdentifier($bucketDatabaseName)
        ));
        $shareRoleName = $bucketDatabaseName . '_SHARE';
        // cleaning of the share role
        $this->dropRole($sourceProjectConnection, $shareRoleName);

        $sourceProjectConnection->executeQuery('CREATE TABLE TESTTABLE_BEFORE (ID INT)');
        $sourceProjectConnection->executeQuery('INSERT INTO TESTTABLE_BEFORE (1)');

        $targetProjectConnection = $this->getConnection($this->targetProjectCredentials);

        // check that the Project2 cannot access the table yet
        try {
            $targetProjectConnection->fetchAllAssociative(
                sprintf(
                    'SELECT * FROM %s.%s',
                    TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                    TeradataQuote::quoteSingleIdentifier('TESTTABLE_BEFORE')
                )
            );
            $this->fail('Should fail. Bucket is not linked yet');
        } catch (Throwable $e) {
            $this->assertStringContainsString('The user does not have SELECT access to', $e->getMessage());
        }

        // share the bucket
        $handler = new ShareBucketHandler($this->sessionManager);
        $command = (new ShareBucketCommand())
            ->setBucketObjectName($bucketDatabaseName)
            ->setProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName())
            ->setBucketShareRoleName($shareRoleName);

        $handler(
            $this->sourceProjectCredentials,
            $command,
            []
        );

        // link the bucket
        $handler = new LinkBucketHandler($this->sessionManager);
        $command = (new LinkBucketCommand())
            ->setBucketObjectName($bucketDatabaseName)
            ->setSourceShareRoleName($shareRoleName)
            ->setProjectReadOnlyRoleName($this->targetProjectResponse->getProjectReadOnlyRoleName());

        // soure project has to link it
        $handler(
            $this->sourceProjectCredentials,
            $command,
            []
        );

        // check that there is no need to re-share or whatever
        $sourceProjectConnection->executeQuery('CREATE TABLE TESTTABLE_AFTER (ID INT)');
        $sourceProjectConnection->executeQuery('INSERT INTO TESTTABLE_AFTER (1)');

        $dataBefore = $targetProjectConnection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                TeradataQuote::quoteSingleIdentifier('TESTTABLE_BEFORE')
            )
        );

        $dataAfter = $targetProjectConnection->fetchAllAssociative(
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
