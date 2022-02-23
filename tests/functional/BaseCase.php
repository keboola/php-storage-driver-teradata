<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Google\Protobuf\Any;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Teradata\ConnectionFactory;
use Keboola\StorageDriver\Teradata\Handler\Project\Create\CreateProjectHandler;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use PHPUnit\Framework\TestCase;

class BaseCase extends TestCase
{
    protected const PROJECT_USER_SUFFIX = '_KBC_user';
    protected const PROJECT_ROLE_SUFFIX = '_KBC_role';
    protected const PROJECT_READ_ONLY_ROLE_SUFFIX = '_KBC_RO';
    protected const PROJECT_PASSWORD = 'PassW0rd#';

    /**
     * Check if parent has child
     */
    protected function isChildOf(Connection $connection, string $child, string $parent): bool
    {
        $exists = $connection->fetchAllAssociative(sprintf(
            'SELECT * FROM DBC.ChildrenV WHERE Child = %s AND Parent = %s;',
            TeradataQuote::quote($child),
            TeradataQuote::quote($parent)
        ));

        return count($exists) === 1;
    }

    protected function cleanTestProject(): void
    {
        $db = $this->getConnection($this->getCredentials());
        $this->cleanUser($db, $this->getProjectUser());
        $this->dropRole($db, $this->getProjectReadOnlyRole());
        $this->dropRole($db, $this->getProjectRole());
    }

    protected function getConnection(GenericBackendCredentials $credentials): Connection
    {
        return ConnectionFactory::getConnection($credentials);
    }

    /**
     * Get credentials from envs
     */
    protected function getCredentials(): GenericBackendCredentials
    {
        $any = new Any();
        $any->pack((new GenericBackendCredentials\TeradataCredentialsMeta())->setDatabase(
            $this->getRootDatabase()
        ));

        return (new GenericBackendCredentials())
            ->setHost((string) getenv('TERADATA_HOST'))
            ->setPrincipal((string) getenv('TERADATA_USERNAME'))
            ->setSecret((string) getenv('TERADATA_PASSWORD'))
            ->setPort((int) getenv('TERADATA_PORT'))
            ->setMeta($any);
    }

    protected function getRootDatabase(): string
    {
        return (string) getenv('TERADATA_ROOT_DATABASE');
    }

    /**
     * Drop user and child objects
     */
    protected function cleanUser(Connection $connection, string $name): void
    {
        if (!$this->isUserExists($connection, $name)) {
            return;
        }

        // delete all objects in the DB
        $connection->executeStatement(sprintf('DELETE USER %s ALL', TeradataQuote::quoteSingleIdentifier($name)));
        // drop the empty db
        $connection->executeStatement(sprintf('DROP USER %s', TeradataQuote::quoteSingleIdentifier($name)));
    }

    /**
     * Check if user exists
     */
    protected function isUserExists(Connection $connection, string $name): bool
    {
        try {
            $connection->executeStatement(sprintf('HELP USER %s', TeradataQuote::quoteSingleIdentifier($name)));
            return true;
        } catch (Exception $e) {
            return false;
        }
    }

    protected function getProjectUser(): string
    {
        return md5($this->getName()) . self::PROJECT_USER_SUFFIX;
    }

    /**
     * Drop role if exists
     */
    protected function dropRole(Connection $connection, string $roleName): void
    {
        if (!$this->isRoleExists($connection, $roleName)) {
            return;
        }

        $connection->executeQuery(sprintf(
            'DROP ROLE %s',
            TeradataQuote::quoteSingleIdentifier($roleName)
        ));
    }

    /**
     * check if role exists
     */
    protected function isRoleExists(Connection $connection, string $roleName): bool
    {
        $roles = $connection->fetchAllAssociative(sprintf(
            'SELECT RoleName FROM DBC.RoleInfoV WHERE RoleName = %s',
            TeradataQuote::quote($roleName)
        ));
        return count($roles) === 1;
    }

    protected function getProjectReadOnlyRole(): string
    {
        return md5($this->getName()) . self::PROJECT_READ_ONLY_ROLE_SUFFIX;
    }

    protected function getProjectRole(): string
    {
        return md5($this->getName()) . self::PROJECT_ROLE_SUFFIX;
    }

    /**
     * Create test project scoped for each test name
     */
    protected function createTestProject(): void
    {
        $handler = new CreateProjectHandler();
        $meta = new Any();
        $meta->pack((new CreateProjectCommand\CreateProjectTeradataMeta())->setRootDatabase(
            $this->getRootDatabase()
        ));
        $command = (new CreateProjectCommand())
            ->setProjectUser($this->getProjectUser())
            ->setProjectPassword(self::PROJECT_PASSWORD)
            ->setProjectRole($this->getProjectRole())
            ->setReadOnlyRoleName($this->getProjectReadOnlyRole())
            ->setMeta($meta);

        $handler(
            $this->getCredentials(),
            $command,
            []
        );
    }

    /**
     * Get test project credentials scoped for each test name
     */
    protected function getTestProjectCredentials(): GenericBackendCredentials
    {
        $rootCredentials = $this->getCredentials();
        return (new GenericBackendCredentials())
            ->setHost($rootCredentials->getHost())
            ->setPrincipal($this->getProjectUser())
            ->setSecret(self::PROJECT_PASSWORD)
            ->setPort($rootCredentials->getPort());
    }
}
