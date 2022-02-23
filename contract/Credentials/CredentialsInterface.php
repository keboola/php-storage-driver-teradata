<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Credentials;

use Keboola\StorageDriver\Contract\Credentials\Meta\CredentialsMetaInterface;

interface CredentialsInterface
{
    public function host(): string;

    public function principal(): string;

    public function secret(): string;

    public function port(): ?int;

    public function meta(): ?CredentialsMetaInterface;

    /**
     * @return array<mixed>
     */
    public function toArray(): array;
}
