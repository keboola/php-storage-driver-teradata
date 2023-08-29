<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Bucket\Create;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\BackendSupportsInterface;
use Keboola\StorageDriver\Shared\Driver\MetaHelper;
use Keboola\StorageDriver\Shared\NameGenerator\NameGeneratorFactory;
use Keboola\StorageDriver\Teradata\Handler\Exception\ExceptionResolver;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class CreateBucketHandler implements DriverCommandHandlerInterface
{
    public const DEFAULT_PERM_SPACE_SIZE = 1e8; // 100MB;
    public const DEFAULT_SPOOL_SPACE_SIZE = 1e8; // 100MB;

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
        Message $credentials, // project credentials
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateBucketCommand);
        assert($runtimeOptions->getRunId() === '');
        assert($runtimeOptions->getMeta() === null);

        /** @var CreateBucketCommand\CreateBucketTeradataMeta|null $meta */
        $meta = MetaHelper::getMetaRestricted($command, CreateBucketCommand\CreateBucketTeradataMeta::class);
        $permSpace = self::DEFAULT_PERM_SPACE_SIZE;
        $spoolSpace = self::DEFAULT_SPOOL_SPACE_SIZE;
        if ($meta !== null) {
            $permSpace = $meta->getPermSpace() !== '' ? $meta->getPermSpace() : $permSpace;
            $spoolSpace = $meta->getSpoolSpace() !== '' ? $meta->getSpoolSpace() : $permSpace;
        }
        $nameGenerator = NameGeneratorFactory::getGeneratorForBackendAndPrefix(
            BackendSupportsInterface::BACKEND_TERADATA,
            $command->getStackPrefix()
        );
        $newBucketDatabaseName = $nameGenerator->createObjectNameForBucketInProject(
            $command->getBucketId(),
            $command->getProjectId()
        );

        $db = $this->manager->createSession($credentials);
        try {
            $db->executeStatement(sprintf(
                'CREATE DATABASE %s AS '
                . 'PERMANENT = %s, SPOOL = %s;',
                TeradataQuote::quoteSingleIdentifier($newBucketDatabaseName),
                $permSpace,
                $spoolSpace
            ));
        } catch (\Throwable $e) {
            throw ExceptionResolver::resolveException($e);
        }

        // grant select to read only role
        $db->executeStatement(sprintf(
            'GRANT SELECT ON %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($newBucketDatabaseName),
            TeradataQuote::quoteSingleIdentifier($command->getProjectReadOnlyRoleName()),
        ));

        $db->close();

        return (new CreateBucketResponse())
            ->setCreateBucketObjectName($newBucketDatabaseName);
    }
}
