<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\Protobuf\Any;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\DropBucketCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Bucket\Drop\DropBucketHandler;
use Keboola\StorageDriver\Teradata\Handler\Exception\NoSpaceException;
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
        [$response,] = $this->createTestBucket($this->projectCredentials, $this->projectResponse);

        $handler = new DropBucketHandler($this->sessionManager);
        $command = (new DropBucketCommand())
            ->setBucketObjectName($response->getCreateBucketObjectName())
            ->setProjectReadOnlyRoleName($this->projectResponse->getProjectReadOnlyRoleName());

        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $db = $this->getConnection($this->projectCredentials);
        $this->assertFalse($this->isDatabaseExists($db, $response->getCreateBucketObjectName()));

        $db->close();
    }

    public function testCreateDropCascadeBucket(): void
    {
        [$response, $db] = $this->createTestBucket($this->projectCredentials, $this->projectResponse);

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
        $db = $this->getConnection($this->projectCredentials);
        $this->assertTrue($this->isDatabaseExists($db, $response->getCreateBucketObjectName()));

        // ignore errors should not fail but database is not removed
        $command->setIgnoreErrors(true);
        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $db = $this->getConnection($this->projectCredentials);
        $this->assertTrue($this->isDatabaseExists($db, $response->getCreateBucketObjectName()));

        // should not fail and database will be deleted
        $command->setIgnoreErrors(false);
        $command->setIsCascade(true);
        $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $db = $this->getConnection($this->projectCredentials);
        $this->assertFalse($this->isDatabaseExists($db, $response->getCreateBucketObjectName()));

        $db->close();
    }

    public function testCreateTooBigBucket(): void
    {
        $bucket = md5($this->getName()) . '_Test_bucket';

        $meta = new Any();
        $meta->pack(
            (new CreateBucketCommand\CreateBucketTeradataMeta())
                ->setPermSpace('10e15')
                ->setSpoolSpace('10e15')
        );

        $handler = new CreateBucketHandler($this->sessionManager);
        $command = (new CreateBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setProjectId($this->getProjectId())
            ->setBucketId($bucket)
            ->setProjectRoleName($this->projectResponse->getProjectRoleName())
            ->setMeta($meta)
            ->setProjectReadOnlyRoleName($this->projectResponse->getProjectReadOnlyRoleName());

        try {
            $handler(
                $this->getCredentials(),
                $command,
                []
            );
            $this->fail('should fail');
        } catch (Throwable $e) {
            $this->assertInstanceOf(NoSpaceException::class, $e);
            $this->assertEquals(ExceptionInterface::ERR_RESOURCE_FULL, $e->getCode());
            $this->assertEquals('Cannot create database because parent database is full.', $e->getMessage());
        }
    }
}
