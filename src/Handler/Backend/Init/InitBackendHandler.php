<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Backend\Init;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Backend\InitBackendResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\StorageDriver\Teradata\ConnectionFactory;

final class InitBackendHandler implements DriverCommandHandlerInterface
{
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
        assert($command instanceof InitBackendCommand);

        $db = ConnectionFactory::getConnection($credentials);

        $db->fetchOne('SELECT 1');

        /** @var string[] $rights */
        $rights = $db->fetchFirstColumn("SELECT AccessRight FROM DBC.UserRightsV WHERE TableName = 'All'");

        // check create user
        $this->checkAccessRight($rights, 'CU', $credentials->getPrincipal());
        // check drop user
        $this->checkAccessRight($rights, 'DU', $credentials->getPrincipal());

        // check create role
        $this->checkAccessRight($rights, 'CR', $credentials->getPrincipal());
        // check drop role
        $this->checkAccessRight($rights, 'DR', $credentials->getPrincipal());

        // check create database
        $this->checkAccessRight($rights, 'CD', $credentials->getPrincipal());
        // check drop database
        $this->checkAccessRight($rights, 'DD', $credentials->getPrincipal());

        // check create table
        $this->checkAccessRight($rights, 'CT', $credentials->getPrincipal());
        // check drop table
        $this->checkAccessRight($rights, 'DT', $credentials->getPrincipal());

        // check create view
        $this->checkAccessRight($rights, 'CV', $credentials->getPrincipal());
        // check drop view
        $this->checkAccessRight($rights, 'DV', $credentials->getPrincipal());

        return new InitBackendResponse();
    }

    /**
     * @param string[] $rights
     * @throws Exception
     */
    private function checkAccessRight(array $rights, string $right, string $user): void
    {
        if (!in_array($right, $rights)) {
            throw new Exception(sprintf(
                'Missing rights "%s" for user "%s".',
                $right,
                $user
            ));
        }
    }
}
