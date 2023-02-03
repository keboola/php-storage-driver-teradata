<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

class DbUtils
{
    /**
     * Drop user and child objects
     *
     * @param string $cleanerUser - Connected user which will be granted access to drop user/database
     */
    public static function cleanUserOrDatabase(
        Connection $connection,
        string $name,
        string $cleanerUser,
        bool $keepYourself = false
    ): void {
        $isUserExists = self::isUserExists($connection, $name);
        $isDatabaseExists = self::isDatabaseExists($connection, $name);

        if (!$isUserExists && !$isDatabaseExists) {
            return;
        }

        $connection->executeStatement(sprintf(
            'GRANT ALL ON %s TO %s',
            TeradataQuote::quoteSingleIdentifier($name),
            $cleanerUser
        ));
        $connection->executeStatement(sprintf(
            'GRANT DROP USER, DROP DATABASE ON %s TO %s',
            TeradataQuote::quoteSingleIdentifier($name),
            $cleanerUser
        ));
        if ($isUserExists) {
            $connection->executeStatement(sprintf('DELETE USER %s ALL', TeradataQuote::quoteSingleIdentifier($name)));
            $childDatabases = self::getChildDatabases($connection, $name);
            foreach ($childDatabases as $childDatabase) {
                self::cleanUserOrDatabase($connection, $childDatabase, $cleanerUser);
            }
            if ($keepYourself === false) {
                $connection->executeStatement(sprintf(
                    'DROP USER %s',
                    TeradataQuote::quoteSingleIdentifier($name),
                ));
            }
        } else {
            $connection->executeStatement(sprintf(
                'DELETE DATABASE %s ALL',
                TeradataQuote::quoteSingleIdentifier($name)
            ));
            $childDatabases = self::getChildDatabases($connection, $name);
            foreach ($childDatabases as $childDatabase) {
                self::cleanUserOrDatabase($connection, $childDatabase, $cleanerUser);
            }
            if ($keepYourself === false) {
                $connection->executeStatement(sprintf(
                    'DROP DATABASE %s',
                    TeradataQuote::quoteSingleIdentifier($name),
                ));
            }
        }
    }

    /**
     * Check if user exists
     */
    public static function isUserExists(Connection $connection, string $name): bool
    {
        $data = $connection->fetchAllAssociative(
            sprintf(
                "SELECT * FROM DBC.DatabasesV t WHERE t.DatabaseName = %s AND t.DBKind = 'U'",
                TeradataQuote::quote($name)
            )
        );

        return count($data) > 0;
    }

    /**
     * Check if database exists
     */
    public static function isDatabaseExists(Connection $connection, string $name): bool
    {
        $data = $connection->fetchAllAssociative(
            sprintf(
                "SELECT * FROM DBC.DatabasesV t WHERE t.DatabaseName = %s AND t.DBKind = 'D'",
                TeradataQuote::quote($name)
            )
        );

        return count($data) > 0;
    }

    /**
     * @return string[]
     */
    public static function getChildDatabases(Connection $connection, string $name): array
    {
        /** @var string[] $return */
        $return = $connection->fetchFirstColumn(sprintf(
            'SELECT Child FROM DBC.ChildrenV WHERE Parent = %s;',
            TeradataQuote::quote($name),
        ));
        return $return;
    }

    /**
     * Check if parent has child
     */
    public static function isChildOf(Connection $connection, string $child, string $parent): bool
    {
        $exists = $connection->fetchAllAssociative(sprintf(
            'SELECT * FROM DBC.ChildrenV WHERE Child = %s AND Parent = %s;',
            TeradataQuote::quote($child),
            TeradataQuote::quote($parent)
        ));

        return count($exists) === 1;
    }

    /**
     * Drop role if exists
     */
    public static function dropRole(Connection $connection, string $roleName): void
    {
        if (!self::isRoleExists($connection, $roleName)) {
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
    public static function isRoleExists(Connection $connection, string $roleName): bool
    {
        $roles = $connection->fetchAllAssociative(sprintf(
            'SELECT RoleName FROM DBC.RoleInfoV WHERE RoleName = %s',
            TeradataQuote::quote($roleName)
        ));
        return count($roles) === 1;
    }

    public static function isTableExists(Connection $connection, string $databaseName, string $tableName): bool
    {
        $tables = $connection->fetchAllAssociative(sprintf(
            'SELECT TableName FROM DBC.TablesVX WHERE DatabaseName = %s AND TableName = %s',
            TeradataQuote::quote($databaseName),
            TeradataQuote::quote($tableName)
        ));
        return count($tables) === 1;
    }
}
