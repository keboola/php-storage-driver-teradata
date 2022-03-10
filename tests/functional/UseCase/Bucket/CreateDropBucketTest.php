<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Bucket\DropBucketCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Drop\DropBucketHandler;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Throwable;

class CreateDropBucketTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();
        [$credentials, $response] = $this->createTestProject();
        $this->projectCredentials = $credentials;
        $this->projectResponse = $response;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testCreateDropBucket(): void
    {
        $bucket = md5($this->getName()) . '_Test_bucket';

        [$response, $db] = $this->createBucket($bucket);

        $handler = new DropBucketHandler($this->sessionManager);
        $command = (new DropBucketCommand())
            ->setBucketObjectName($response->getCreateBucketObjectName())
            ->setProjectReadOnlyRoleName($this->projectResponse->getProjectReadOnlyRoleName());

        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $this->assertFalse($this->isDatabaseExists($db, $response->getCreateBucketObjectName()));

        $db->close();
    }

    /**
     * @return array{CreateBucketResponse, Connection}
     */
    private function createBucket(string $bucket): array
    {
        $handler = new CreateBucketHandler($this->sessionManager);
        $command = (new CreateBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setProjectId($this->getProjectId())
            ->setBucketId($bucket)
            ->setProjectRoleName($this->projectResponse->getProjectRoleName())
            ->setProjectReadOnlyRoleName($this->projectResponse->getProjectReadOnlyRoleName());

        $response = $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $this->assertInstanceOf(CreateBucketResponse::class, $response);

        $db = $this->getConnection($this->projectCredentials);

        $this->assertTrue($this->isDatabaseExists($db, $response->getCreateBucketObjectName()));

        // read only role has read access to bucket
        $this->assertEqualsArrays(
            ['R '],
            $this->getRoleAccessRightForDatabase(
                $db,
                $this->projectResponse->getProjectReadOnlyRoleName(),
                $response->getCreateBucketObjectName()
            )
        );

        return [$response, $db];
    }

    public function testCreateDropCascadeBucket(): void
    {
        $bucket = md5($this->getName()) . '_Test_bucket';

        [$response, $db] = $this->createBucket($bucket);

        // create table
        $db->executeStatement(sprintf(
            'CREATE TABLE %s."test" ("col1" VARCHAR(5));',
            TeradataQuote::quoteSingleIdentifier($response->getCreateBucketObjectName())
        ));

        $handler = new DropBucketHandler($this->sessionManager);
        $command = (new DropBucketCommand())
            ->setBucketObjectName($response->getCreateBucketObjectName())
            ->setProjectReadOnlyRoleName($this->projectResponse->getProjectReadOnlyRoleName());

        try {
            $handler(
                $this->projectCredentials,
                $command,
                []
            );
            $this->fail('Should fail as bucket database contains table');
        } catch (Throwable $e) {
            $this->assertStringContainsString(
                'Cannot DROP databases with tables, journal tables, views, macros, or zones.',
                $e->getMessage()
            );
        }
        $this->assertTrue($this->isDatabaseExists($db, $response->getCreateBucketObjectName()));

        // ignore errors should not fail but database is not removed
        $command->setIgnoreErrors(true);
        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $this->assertTrue($this->isDatabaseExists($db, $response->getCreateBucketObjectName()));

        // should not fail and database will be deleted
        $command->setIgnoreErrors(false);
        $command->setIsCascade(true);
        $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $this->assertFalse($this->isDatabaseExists($db, $response->getCreateBucketObjectName()));

        $db->close();
    }
}
