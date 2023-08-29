<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata\Handler\Backend\Init;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Backend\InitBackendResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\StorageDriver\Teradata\TeradataAccessRight;
use Keboola\StorageDriver\Teradata\TeradataSessionManager;

final class InitBackendHandler implements DriverCommandHandlerInterface
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
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof InitBackendCommand);
        assert($runtimeOptions->getRunId() === '');
        assert($runtimeOptions->getMeta() === null);

        $db = $this->manager->createSession($credentials);

        $db->fetchOne('SELECT 1');

        /** @var string[] $rights */
        $rights = $db->fetchFirstColumn("SELECT AccessRight FROM DBC.UserRightsV WHERE TableName = 'All'");

        // check create user
        $this->checkAccessRight($rights, TeradataAccessRight::RIGHT_CREATE_USER, $credentials->getPrincipal());
        // check drop user
        $this->checkAccessRight($rights, TeradataAccessRight::RIGHT_DROP_USER, $credentials->getPrincipal());

        // check create role
        $this->checkAccessRight($rights, TeradataAccessRight::RIGHT_CREATE_ROLE, $credentials->getPrincipal());
        // check drop role
        $this->checkAccessRight($rights, TeradataAccessRight::RIGHT_DROP_ROLE, $credentials->getPrincipal());

        // check create database
        $this->checkAccessRight($rights, TeradataAccessRight::RIGHT_CREATE_DATABASE, $credentials->getPrincipal());
        // check drop database
        $this->checkAccessRight($rights, TeradataAccessRight::RIGHT_DROP_DATABASE, $credentials->getPrincipal());

        // check create table
        $this->checkAccessRight($rights, TeradataAccessRight::RIGHT_CREATE_TABLE, $credentials->getPrincipal());
        // check drop table
        $this->checkAccessRight($rights, TeradataAccessRight::RIGHT_DROP_TABLE, $credentials->getPrincipal());

        // check create view
        $this->checkAccessRight($rights, TeradataAccessRight::RIGHT_CREATE_VIEW, $credentials->getPrincipal());
        // check drop view
        $this->checkAccessRight($rights, TeradataAccessRight::RIGHT_DROP_VIEW, $credentials->getPrincipal());

        // check execute specific function
        $this->checkAccessRight($rights, TeradataAccessRight::RIGHT_EXECUTE, $credentials->getPrincipal());

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
