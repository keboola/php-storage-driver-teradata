<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Contract\Credentials\GenericBackendCredentials;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataConnection;

class ConnectionFactory
{
    public static function getConnection(GenericBackendCredentials $credentials): Connection
    {
        return TeradataConnection::getConnection([
            'host' => $credentials->host(),
            'user' => $credentials->principal(),
            'password' => $credentials->secret(),
            'port' => $credentials->port() ?? 1025,
            'dbname' => '',
        ]);
    }
}
