<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command\Bucket\Drop;

use Keboola\StorageDriver\Contract\Driver\Command\AbstractCommand;
use Keboola\StorageDriver\Contract\Driver\Command\Bucket\Create\CreateBucketCommandMeta;

class DropBucketCommand extends AbstractCommand
{
    public const NAME = 'bucket:drop';

    private string $name;

    private CreateBucketCommandMeta $meta;

    private bool $ignoreErrors;

    public function __construct(
        string $name,
        CreateBucketCommandMeta $meta,
        bool $ignoreErrors = false
    ) {
        $this->name = $name;
        $this->meta = $meta;
        $this->ignoreErrors = $ignoreErrors;
    }

    public function toArray(): array
    {
        return [
            'name' => $this->name,
            'ignoreErrors' => $this->ignoreErrors,
            'meta' => $this->meta->toArray(),
        ];
    }
}
