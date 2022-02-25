<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Handler;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand\CreateBucketTeradataMeta;
use Keboola\StorageDriver\Teradata\Handler\MetaHelper;
use PHPUnit\Framework\TestCase;
use Throwable;

class MetaHelperTest extends TestCase
{
    public function testNotContainMeta(): void
    {
        $result = MetaHelper::getMetaFromCommand(
            $this->createMock(Message::class),
            InitBackendCommand\InitBackendSynapseMeta::class
        );
        $this->assertNull($result);
    }

    public function testNoMetaSet(): void
    {
        $result = MetaHelper::getMetaFromCommand(
            new InitBackendCommand(),
            InitBackendCommand\InitBackendSynapseMeta::class
        );
        $this->assertNull($result);
    }

    public function testInvalidMetaInstance(): void
    {
        $meta = new Any();
        $meta->pack(
            new InitBackendCommand\InitBackendSynapseMeta()
        );

        $this->expectException(Throwable::class);
        MetaHelper::getMetaFromCommand(
            (new InitBackendCommand())->setMeta($meta),
            CreateBucketTeradataMeta::class
        );
    }

    public function testCorrectMetaInstance(): void
    {
        $meta = new Any();
        $meta->pack(
            (new InitBackendCommand\InitBackendSynapseMeta())
                ->setGlobalRoleName('test')
        );

        $result = MetaHelper::getMetaFromCommand(
            (new InitBackendCommand())->setMeta($meta),
            InitBackendCommand\InitBackendSynapseMeta::class
        );
        $this->assertInstanceOf(InitBackendCommand\InitBackendSynapseMeta::class, $result);
        $this->assertSame('test', $result->getGlobalRoleName());
    }
}
