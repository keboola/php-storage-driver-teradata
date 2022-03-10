<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Table\Drop;

use Doctrine\DBAL\Exception;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\ConnectionFactory;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;

final class DropTableHandler implements DriverCommandHandlerInterface
{
    private TeradataSessionManager $manager;

    public function __construct(TeradataSessionManager $manager)
    {
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropTableCommand);

        // validate
        assert($command->getPath()->count() > 0, 'DropTableCommand.path is required');
        assert(!empty($command->getTableName()), 'DropTableCommand.tableName is required');

        // get bucket database
        $paths = [];
        foreach ($command->getPath() as $path) {
            $paths[] = $path;
        }
        $databaseName = implode('.', $paths);

        $db = $this->manager->createSession($credentials);

        // build sql
        $builder = new TeradataTableQueryBuilder();
        $dropTableSql = $builder->getDropTableCommand($databaseName, $command->getTableName());

        // drop table
        try {
            $db->executeStatement($dropTableSql);
        } catch (Exception $e) {
            if (!$command->getIgnoreErrors()) {
                throw $e;
            }
        }

        $db->close();
        return null;
    }
}
