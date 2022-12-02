<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Logging\Middleware;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataConnection;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Psr\Log\LogLevel;

class ConnectionFactory
{
    public static function getConnection(GenericBackendCredentials $credentials, bool $debug = false): Connection
    {
        $cfg = new Configuration();
        if ($debug === true) {
            $cfg->setMiddlewares([new Middleware(new DebugLogger())]);
            error_log(
                sprintf(
                    '%s: Logging as user: "%s"',
                    strtoupper(LogLevel::INFO),
                    $credentials->getPrincipal()
                )
            );
        }
        $connection = TeradataConnection::getConnection([
            'host' => $credentials->getHost(),
            'user' => $credentials->getPrincipal(),
            'password' => $credentials->getSecret(),
            'port' => $credentials->getPort() === 0 ? 1025 : $credentials->getPort(),
            'dbname' => '',
            'serverVersion' => '17.10',
        ], $cfg);
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

        // apply roles permission to the logged user
        $connection->executeStatement('SET ROLE ALL;');

        return $connection;
    }
}
