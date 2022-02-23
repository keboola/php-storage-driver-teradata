<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Credentials\Meta;


interface CredentialsMetaInterface
{
    /**
     * @return array<mixed>
     */
    public function toArray(): array;
}
