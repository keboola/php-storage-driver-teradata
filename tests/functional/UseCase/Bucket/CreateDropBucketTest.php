<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\DropBucketCommand;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Drop\DropBucketHandler;

class CreateDropBucketTest extends BaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();
        $this->createTestProject();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testCreateDropBucket(): void
    {
        $credentials = $this->getTestProjectCredentials();

        $bucket = md5($this->getName()) . '_Test_bucket';

        $handler = new CreateBucketHandler();
        $command = (new CreateBucketCommand())
            ->setBucketName($bucket)
            ->setProjectRole($this->getProjectRole())
            ->setProjectReadOnlyRole($this->getProjectReadOnlyRole());

        $handler(
            $credentials,
            $command,
            []
        );

        $db = $this->getConnection($credentials);

        $this->assertTrue($this->isDatabaseExists($db, $bucket));

        // read only role has read access to bucket
        $this->assertEqualsArrays(
            ['R '],
            $this->getRoleAccessRightForDatabase($db, $this->getProjectReadOnlyRole(), $bucket)
        );

        $db->close();

        $handler = new DropBucketHandler();
        $command = (new DropBucketCommand())
            ->setBucketName($bucket)
            ->setProjectReadOnlyRole($this->getProjectReadOnlyRole());

        $handler(
            $credentials,
            $command,
            []
        );

        $this->assertFalse($this->isDatabaseExists($db, $bucket));
    }
}
