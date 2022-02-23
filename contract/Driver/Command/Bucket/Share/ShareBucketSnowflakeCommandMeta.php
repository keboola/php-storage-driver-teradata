<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command\Bucket\Share;

use Keboola\StorageDriver\Contract\Driver\Command\CommandMetaInterface;

class ShareBucketSnowflakeCommandMeta implements CommandMetaInterface
{
    private string $databaseName;

    public function __construct(
        string $databaseName
    ) {
        $this->databaseName = $databaseName;
    }

    public function toArray(): array
    {
        return [
            'databaseName' => $this->databaseName,
        ];
    }
}
