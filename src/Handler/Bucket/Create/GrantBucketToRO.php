<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Bucket\Create;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Bucket\GrandReadOnlyRoleToBucketCommand;
use Keboola\StorageDriver\Command\Workspace\GrantWorkspaceAccessToProjectCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\BackendSupportsInterface;
use Keboola\StorageDriver\Shared\Driver\MetaHelper;
use Keboola\StorageDriver\Shared\NameGenerator\NameGeneratorFactory;
use Keboola\StorageDriver\Teradata\Handler\Exception\ExceptionResolver;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class GrantBucketToRO implements DriverCommandHandlerInterface
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
        Message $credentials, // project credentials
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof GrandReadOnlyRoleToBucketCommand);


        $db = $this->manager->createSession($credentials);
        try {
            $db->executeQuery(sprintf(
                'GRANT SELECT ON %s TO %s',
                TeradataQuote::quoteSingleIdentifier($command->getBucketObjectName()),
                TeradataQuote::quoteSingleIdentifier($command->getProjectReadOnlyRoleName()),
            ));
        } catch (\Throwable $e) {
            throw ExceptionResolver::resolveException($e);
        }

        return null;
    }
}
