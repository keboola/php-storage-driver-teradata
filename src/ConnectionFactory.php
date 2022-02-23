<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataConnection;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

class ConnectionFactory
{
    public static function getConnection(GenericBackendCredentials $credentials): Connection
    {
        $connection = TeradataConnection::getConnection([
            'host' => $credentials->getHost(),
            'user' => $credentials->getPrincipal(),
            'password' => $credentials->getSecret(),
            'port' => $credentials->getPort() === 0 ? 1025 : $credentials->getPort(),
            'dbname' => '',
        ]);
        $meta = $credentials->getMeta();
        if ($meta !== null) {
            $meta = $meta->unpack();
            assert($meta instanceof GenericBackendCredentials\TeradataCredentialsMeta);
            $databaseName = $meta->getDatabase();
            $connection->executeStatement(sprintf(
                'SET SESSION DATABASE %s;',
                TeradataQuote::quoteSingleIdentifier($databaseName)
            ));
        }

        return $connection;
    }
}
