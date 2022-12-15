<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketResponse;
use Keboola\StorageDriver\Command\Bucket\UnlinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\UnshareBucketCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Link\LinkBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Share\ShareBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Bucket\UnLink\UnLinkBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Bucket\UnShare\UnShareBucketHandler;
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
        $expectedShareRoleName = 'KBC_PROJECT456_BUCKET123_SHARE';
        // cleaning of the share role
        $this->dropRole($sourceProjectConnection, $expectedShareRoleName);

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
            ->setStackPrefix('KBC')
            ->setSourceBucketObjectName($bucketDatabaseName)
            ->setSourceBucketId('bucket123')
            ->setSourceProjectId('project456')
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $shareResponse = $handler(
            $this->sourceProjectCredentials,
            $command,
            []
        );

        $this->assertInstanceOf(ShareBucketResponse::class, $shareResponse);
        $this->assertEquals($expectedShareRoleName, $shareResponse->getBucketShareRoleName());

        // link the bucket
        $handler = new LinkBucketHandler($this->sessionManager);
        $command = (new LinkBucketCommand())
            ->setSourceShareRoleName($expectedShareRoleName)
            ->setTargetProjectReadOnlyRoleName($this->targetProjectResponse->getProjectReadOnlyRoleName());

        // it is sourceProject who does the grants -> that's why the sourceProjectCredentials
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

        // unlink and check that target project cannot access it anymore
        $unlinkHandler = new UnLinkBucketHandler($this->sessionManager);
        $command = (new UnLinkBucketCommand())
            ->setBucketObjectName($bucketDatabaseName)
            ->setProjectReadOnlyRoleName($this->targetProjectResponse->getProjectReadOnlyRoleName())
            ->setSourceShareRoleName($expectedShareRoleName);

        $unlinkHandler(
            $this->sourceProjectCredentials,
            $command,
            []
        );

        // check that the Project2 cannot access the table anymore
        try {
            $targetProjectConnection->fetchAllAssociative(
                sprintf(
                    'SELECT * FROM %s.%s',
                    TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                    TeradataQuote::quoteSingleIdentifier('TESTTABLE_BEFORE')
                )
            );
            $this->fail('Should fail. Bucket has been unlinked');
        } catch (Throwable $e) {
            $this->assertStringContainsString('The user does not have SELECT access to', $e->getMessage());
        }
    }

    public function testShareUnshare(): void
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
        $expectedShareRoleName = 'KBC_PROJECT456_BUCKET123_SHARE';
        // cleaning of the share role
        $this->dropRole($sourceProjectConnection, $expectedShareRoleName);

        $handler = new ShareBucketHandler($this->sessionManager);
        $command = (new ShareBucketCommand())
            ->setStackPrefix('KBC')
            ->setSourceBucketId('bucket123')
            ->setSourceProjectId('project456')
            ->setSourceBucketObjectName($bucketDatabaseName)
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $shareResponse = $handler(
            $this->sourceProjectCredentials,
            $command,
            []
        );

        $this->assertInstanceOf(ShareBucketResponse::class, $shareResponse);
        $this->assertEquals($expectedShareRoleName, $shareResponse->getBucketShareRoleName());

        $this->assertTrue($this->isRoleExists($sourceProjectConnection, $expectedShareRoleName));

        $handler = new UnShareBucketHandler($this->sessionManager);
        $command = (new UnShareBucketCommand())
            ->setBucketObjectName($bucketDatabaseName)
            ->setBucketShareRoleName($expectedShareRoleName);

        $handler(
            $this->sourceProjectCredentials,
            $command,
            []
        );

        $this->assertFalse($this->isRoleExists($sourceProjectConnection, $expectedShareRoleName));
    }

    public function testShareUnshareLinkedBucket(): void
    {
        [$bucketResponse, $sourceProjectConnection] = $this->createTestBucket(
            $this->sourceProjectCredentials,
            $this->sourceProjectResponse
        );
        $expectedShareRoleName = 'KBC_PROJECT456_BUCKET123_SHARE';

        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();

        // $projectConnection doesnt have set bucket DB yet
        $sourceProjectConnection->executeStatement(sprintf(
            'SET SESSION DATABASE %s;',
            TeradataQuote::quoteSingleIdentifier($bucketDatabaseName)
        ));
        // cleaning of the share role
        $this->dropRole($sourceProjectConnection, $expectedShareRoleName);

        $handler = new ShareBucketHandler($this->sessionManager);
        $command = (new ShareBucketCommand())
            ->setStackPrefix('KBC')
            ->setSourceBucketObjectName($bucketDatabaseName)
            ->setSourceBucketId('bucket123')
            ->setSourceProjectId('project456')
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $shareResponse = $handler(
            $this->sourceProjectCredentials,
            $command,
            []
        );

        $this->assertInstanceOf(ShareBucketResponse::class, $shareResponse);
        $this->assertEquals($expectedShareRoleName, $shareResponse->getBucketShareRoleName());

        // check that there is no need to re-share or whatever
        $sourceProjectConnection->executeQuery('CREATE TABLE TESTTABLE_AFTER (ID INT)');
        $sourceProjectConnection->executeQuery('INSERT INTO TESTTABLE_AFTER (1)');

        $this->assertTrue($this->isRoleExists($sourceProjectConnection, $expectedShareRoleName));

        $handler = new LinkBucketHandler($this->sessionManager);
        $command = (new LinkBucketCommand())
            ->setSourceShareRoleName($expectedShareRoleName)
            ->setTargetProjectReadOnlyRoleName($this->targetProjectResponse->getProjectReadOnlyRoleName());

        $handler(
            $this->sourceProjectCredentials,
            $command,
            []
        );

        $handler = new UnShareBucketHandler($this->sessionManager);
        $command = (new UnShareBucketCommand())
            ->setBucketObjectName($bucketDatabaseName)
            ->setBucketShareRoleName($expectedShareRoleName);

        $handler(
            $this->sourceProjectCredentials,
            $command,
            []
        );

        $this->assertFalse($this->isRoleExists($sourceProjectConnection, $expectedShareRoleName));

        $targetProjectConnection = $this->getConnection($this->targetProjectCredentials);
        // check that the Project2 cannot access the table anymore
        try {
            $targetProjectConnection->fetchAllAssociative(
                sprintf(
                    'SELECT * FROM %s.%s',
                    TeradataQuote::quoteSingleIdentifier($bucketDatabaseName),
                    TeradataQuote::quoteSingleIdentifier('TESTTABLE_AFTER')
                )
            );
            $this->fail('Should fail. Bucket has been unshared');
        } catch (Throwable $e) {
            $this->assertStringContainsString('The user does not have SELECT access to', $e->getMessage());
        }
    }
}
