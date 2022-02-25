<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Contract\Driver;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand\CreateBucketTeradataMeta;
use Keboola\StorageDriver\Contract\Driver\MetaHelper;
use PHPUnit\Framework\TestCase;
use Throwable;

class MetaHelperTest extends TestCase
{
    public function testGetMetaNotContainMeta(): void
    {
        $result = MetaHelper::getMeta(
            $this->createMock(Message::class)
        );
        $this->assertNull($result);
    }

    public function testGetMetaNoMetaSet(): void
    {
        $result = MetaHelper::getMeta(
            new InitBackendCommand()
        );
        $this->assertNull($result);
    }

    public function testGetMetaCorrectMetaInstance(): void
    {
        $meta = new Any();
        $meta->pack(
            (new InitBackendCommand\InitBackendSynapseMeta())
                ->setGlobalRoleName('test')
        );

        $result = MetaHelper::getMeta(
            (new InitBackendCommand())->setMeta($meta)
        );
        $this->assertInstanceOf(InitBackendCommand\InitBackendSynapseMeta::class, $result);
        $this->assertSame('test', $result->getGlobalRoleName());
    }

    public function testGetMetaRestrictedNotContainMeta(): void
    {
        $result = MetaHelper::getMetaRestricted(
            $this->createMock(Message::class),
            InitBackendCommand\InitBackendSynapseMeta::class
        );
        $this->assertNull($result);
    }

    public function testGetMetaRestrictedNoMetaSet(): void
    {
        $result = MetaHelper::getMetaRestricted(
            new InitBackendCommand(),
            InitBackendCommand\InitBackendSynapseMeta::class
        );
        $this->assertNull($result);
    }

    public function testGetMetaRestrictedInvalidMetaInstance(): void
    {
        $meta = new Any();
        $meta->pack(
            new InitBackendCommand\InitBackendSynapseMeta()
        );

        $this->expectException(Throwable::class);
        MetaHelper::getMetaRestricted(
            (new InitBackendCommand())->setMeta($meta),
            CreateBucketTeradataMeta::class
        );
    }

    public function testGetMetaRestrictedCorrectMetaInstance(): void
    {
        $meta = new Any();
        $meta->pack(
            (new InitBackendCommand\InitBackendSynapseMeta())
                ->setGlobalRoleName('test')
        );

        $result = MetaHelper::getMetaRestricted(
            (new InitBackendCommand())->setMeta($meta),
            InitBackendCommand\InitBackendSynapseMeta::class
        );
        $this->assertInstanceOf(InitBackendCommand\InitBackendSynapseMeta::class, $result);
        $this->assertSame('test', $result->getGlobalRoleName());
    }
}
