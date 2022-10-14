<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Logging\Middleware;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\DebugLogger;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataConnection;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Psr\Log\LoggerInterface;

class ConnectionFactory
{
    public static function getConnection(
        GenericBackendCredentials $credentials,
        bool $debug = false,
        ?LoggerInterface $debugLogger = null
    ): Connection {
        $cfg = new Configuration();
        if ($debug === true) {
            if ($debugLogger === null) {
                $debugLogger = new DebugLogger();
            }
            $cfg->setMiddlewares([new Middleware($debugLogger)]);
            $debugLogger->debug(
                sprintf(
                    'Logging as user: "%s"',
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

        return $connection;
    }
}
