<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Bucket\DropBucketCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Drop\DropBucketHandler;

class CreateDropBucketTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
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

        $handler = new CreateBucketHandler();
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

        $db->close();

        $handler = new DropBucketHandler();
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
}
