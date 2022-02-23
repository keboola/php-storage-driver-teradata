<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command\Project\Init;

use Keboola\StorageDriver\Contract\Driver\Command\AbstractCommand;
use Keboola\StorageDriver\Contract\Driver\Command\Bucket\Create\CreateBucketCommandMeta;

class ProjectInitCommand extends AbstractCommand
{
    public const NAME = 'project:create';

    private string $projectUser;

    private string $projectRole;

    private CreateBucketCommandMeta $meta;

    public function __construct(
        string $projectUser,
        string $projectRole,
        CreateBucketCommandMeta $meta
    ) {
        $this->projectUser = $projectUser;
        $this->projectRole = $projectRole;
        $this->meta = $meta;
    }

    public function toArray(): array
    {
        return [
            'projectUser' => $this->projectUser,
            'projectRole' => $this->projectRole,
            'meta' => $this->meta->toArray(),
        ];
    }
}
