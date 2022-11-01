<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Bucket\Share;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class ShareBucketHandler implements DriverCommandHandlerInterface
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
        array   $features
    ): ?Message
    {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof ShareBucketCommand);

        $db = $this->manager->createSession($credentials);


        $shareRoleName = $command->getBucketShareRoleName();

        if (count($db->fetchAllAssociative(sprintf(
                'SELECT * FROM DBC.RoleInfoV WHERE RoleName = %s;',
                TeradataQuote::quote($shareRoleName)
            ))) === 0) {
            // share role does not exist yet
            $db->executeStatement(sprintf(
                'CREATE ROLE %s;',
                TeradataQuote::quoteSingleIdentifier($shareRoleName)
            ));
        }

        $db->executeStatement(sprintf(
            'GRANT SELECT ON %s TO %s;',
            TeradataQuote::quoteSingleIdentifier($command->getBucketObjectName()),
            TeradataQuote::quoteSingleIdentifier($shareRoleName),
        ));

        return null;
    }
}
