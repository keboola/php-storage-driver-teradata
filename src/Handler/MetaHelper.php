<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;

final class MetaHelper
{
    /**
     * @param class-string $expectedMetaInstance
     */
    public static function getMetaFromCommand(Message $command, string $expectedMetaInstance): ?Message
    {
        if (!method_exists($command, 'getMeta')) {
            return null;
        }

        /** @var Any|null $meta */
        $meta = $command->getMeta();
        if ($meta === null) {
            return null;
        }

        $meta = $meta->unpack();
        assert($meta instanceof $expectedMetaInstance);

        return $meta;
    }
}
