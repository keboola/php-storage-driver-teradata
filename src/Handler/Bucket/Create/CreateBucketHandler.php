<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Bucket\Create;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\ConnectionFactory;
use Keboola\StorageDriver\Teradata\Handler\MetaHelper;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class CreateBucketHandler implements DriverCommandHandlerInterface
{
    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     */
    public function __invoke(
        Message $credentials, // project credentials
        Message $command,
        array $features
    ) {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateBucketCommand);

        /** @var CreateBucketCommand\CreateBucketTeradataMeta|null $meta */
        $meta = MetaHelper::getMetaFromCommand($command, CreateBucketCommand\CreateBucketTeradataMeta::class);
        $permSpace = 6000000; // 10x smaller than default for project
        $spoolSpace = 12000000; // 10x smaller than default for project
        if ($meta !== null) {
            $permSpace = $meta->getPermSpace() !== '' ? $meta->getPermSpace() : $permSpace;
            $spoolSpace = $meta->getSpoolSpace() !== '' ? $meta->getSpoolSpace() : $permSpace;
        }

        $db = ConnectionFactory::getConnection($credentials);

        $db->executeStatement(sprintf(
            'CREATE DATABASE %s AS '
            . 'PERMANENT = %s, SPOOL = %s;',
            TeradataQuote::quoteSingleIdentifier($command->getBucketName()),
            $permSpace,
            $spoolSpace
        ));

        // grant select to read only role
        $db->executeStatement(sprintf(
            'GRANT SELECT ON %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($command->getBucketName()),
            TeradataQuote::quoteSingleIdentifier($command->getProjectReadOnlyRole()),
        ));

        $db->close();
    }
}
