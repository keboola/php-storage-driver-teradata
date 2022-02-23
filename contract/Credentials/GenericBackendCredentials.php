<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Credentials;

use Keboola\StorageDriver\Contract\Credentials\Meta\CredentialsMetaInterface;

class GenericBackendCredentials implements CredentialsInterface
{
    private string $host;

    private string $principal;

    private string $secret;

    private ?int $port;

    private ?CredentialsMetaInterface $meta;

    public function __construct(
        string $host,
        string $principal,
        string $secret,
        ?int $port = null,
        ?CredentialsMetaInterface $meta = null
    ) {
        $this->host = $host;
        $this->principal = $principal;
        $this->secret = $secret;
        $this->port = $port;
        $this->meta = $meta;
    }

    public function meta(): ?CredentialsMetaInterface
    {
        return $this->meta;
    }

    public function toArray(): array
    {
        $return = [
            'host' => $this->host(),
            'principal' => $this->principal(),
            'secret' => $this->secret(),
            'port' => $this->port(),
        ];
        if ($this->meta !== null) {
            $return['meta'] = $this->meta->toArray();
        }
        return $return;
    }

    public function host(): string
    {
        return $this->host;
    }

    public function principal(): string
    {
        return $this->principal;
    }

    public function secret(): string
    {
        return $this->secret;
    }

    public function port(): ?int
    {
        return $this->port;
    }
}
