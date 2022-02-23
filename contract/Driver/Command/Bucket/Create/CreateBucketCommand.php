<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command\Bucket\Create;

use Keboola\StorageDriver\Contract\Driver\Command\AbstractCommand;
use Keboola\StorageDriver\Contract\Driver\Command\Bucket\Create\CreateBucketCommandMeta;

class CreateBucketCommand extends AbstractCommand
{
    public const NAME = 'bucket:create';

    private string $name;

    private CreateBucketCommandMeta $meta;

    public function __construct(
        string $name,
        CreateBucketCommandMeta $meta
    ) {
        $this->name = $name;
        $this->meta = $meta;
    }

    public function toArray(): array
    {
        return [
            'name' => $this->name,
            'meta' => $this->meta->toArray(),
        ];
    }
}
