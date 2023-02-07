<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Result;
use Keboola\StorageDriver\Teradata\DbUtils;
use PHPUnit\Framework\TestCase;

class DbUtilsTest extends TestCase
{
    public function testCleanUserOrDatabaseWithNeitherUserOrDatabaseExists(): void
    {
        $connection = $this->createMock(Connection::class);
        // isUserExists
        $connection
            ->expects($this->exactly(2))
            ->method('fetchAllAssociative')
            ->withConsecutive(
                [$this->stringEndsWith("DBKind = 'U'")], // isUserExists
                [$this->stringEndsWith("DBKind = 'D'")], // isDatabaseExists
            )
            ->willReturnOnConsecutiveCalls(
                [], // isUserExists
                [], // isDatabaseExists
            );
        $connection
            ->expects($this->never())
            ->method('executeStatement');

        DbUtils::cleanUserOrDatabase($connection, 'user', 'dbc');
    }

    public function testCleanUserOrDatabaseWithOnlyDbExists(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->exactly(2))
            ->method('fetchAllAssociative')
            ->withConsecutive(
                [$this->stringEndsWith("DBKind = 'U'")], // isUserExists
                [$this->stringEndsWith("DBKind = 'D'")], // isDatabaseExists
            )
            ->willReturnOnConsecutiveCalls(
                [], // isUserExists
                [ // isDatabaseExists
                    ['database'],
                ],
            );
        $connection
            ->expects($this->once())
            ->method('fetchFirstColumn')
            ->with($this->stringStartsWith('SELECT Child FROM'))
            ->willReturn([]); // getChildDatabases
        $connection
            ->expects($this->exactly(4))
            ->method('executeStatement')
            ->withConsecutive(
                [<<<SQL
                    GRANT ALL ON "database" TO "dbc"
                    SQL],
                [<<<SQL
                    GRANT DROP USER, DROP DATABASE ON "database" TO "dbc"
                    SQL],
                [<<<SQL
                    DELETE DATABASE "database" ALL
                    SQL],
                [<<<SQL
                    DROP DATABASE "database"
                    SQL],
            );

        DbUtils::cleanUserOrDatabase($connection, 'database', 'dbc');
    }

    public function testCleanUserOrDatabaseWithOnlyDbExistsKeepYourself(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->exactly(2))
            ->method('fetchAllAssociative')
            ->withConsecutive(
                [$this->stringEndsWith("DBKind = 'U'")], // isUserExists
                [$this->stringEndsWith("DBKind = 'D'")], // isDatabaseExists
            )
            ->willReturnOnConsecutiveCalls(
                [], // isUserExists
                [ // isDatabaseExists
                    ['database'],
                ],
            );
        $connection
            ->expects($this->once())
            ->method('fetchFirstColumn')
            ->with($this->stringStartsWith('SELECT Child FROM'))
            ->willReturn([]); // getChildDatabases
        $connection
            ->expects($this->exactly(3))
            ->method('executeStatement')
            ->withConsecutive(
                [<<<SQL
                    GRANT ALL ON "database" TO "dbc"
                    SQL],
                [<<<SQL
                    GRANT DROP USER, DROP DATABASE ON "database" TO "dbc"
                    SQL],
                [<<<SQL
                    DELETE DATABASE "database" ALL
                    SQL],
            );

        DbUtils::cleanUserOrDatabase($connection, 'database', 'dbc', true);
    }

    public function testCleanUserOrDatabaseWithDatabaseContainsChildren(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->exactly(4))
            ->method('fetchAllAssociative')
            ->withConsecutive(
                [$this->stringEndsWith("DBKind = 'U'")], // parent - isUserExists
                [$this->stringEndsWith("DBKind = 'D'")], // parent - isDatabaseExists
                [$this->stringEndsWith("DBKind = 'U'")], // child - isUserExists
                [$this->stringEndsWith("DBKind = 'D'")], // child - isDatabaseExists
            )
            ->willReturnOnConsecutiveCalls(
                [], // parent - isUserExists
                [ // parent - isDatabaseExists
                    ['parent'],
                ],
                [], // child - isUserExists
                [ // child - isDatabaseExists
                    ['child'],
                ],
            );
        $connection
            ->expects($this->exactly(2))
            ->method('fetchFirstColumn')
            ->with($this->stringStartsWith('SELECT Child FROM'))
            ->willReturnOnConsecutiveCalls(
                [ // parent
                    'child',
                ],
                [], // child
            ); // getChildDatabases
        $connection
            ->expects($this->exactly(8))
            ->method('executeStatement')
            ->withConsecutive(
                // parent
                [<<<SQL
                    GRANT ALL ON "parent" TO "dbc"
                    SQL],
                [<<<SQL
                    GRANT DROP USER, DROP DATABASE ON "parent" TO "dbc"
                    SQL],
                [<<<SQL
                    DELETE DATABASE "parent" ALL
                    SQL],
                // child
                [<<<SQL
                    GRANT ALL ON "child" TO "dbc"
                    SQL],
                [<<<SQL
                    GRANT DROP USER, DROP DATABASE ON "child" TO "dbc"
                    SQL],
                [<<<SQL
                    DELETE DATABASE "child" ALL
                    SQL],
                [<<<SQL
                    DROP DATABASE "child"
                    SQL],
                // back to parent
                [<<<SQL
                    DROP DATABASE "parent"
                    SQL],
            );

        DbUtils::cleanUserOrDatabase($connection, 'parent', 'dbc');
    }

    public function testCleanUserOrDatabaseWithUserContainsChildren(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->exactly(4))
            ->method('fetchAllAssociative')
            ->withConsecutive(
                [$this->stringEndsWith("DBKind = 'U'")], // parent - isUserExists
                [$this->stringEndsWith("DBKind = 'D'")], // parent - isDatabaseExists
                [$this->stringEndsWith("DBKind = 'U'")], // child - isUserExists
                [$this->stringEndsWith("DBKind = 'D'")], // child - isDatabaseExists
            )
            ->willReturnOnConsecutiveCalls(
                [ // parent - isUserExists
                    ['parent'],
                ],
                [], // parent - isDatabaseExists
                [ // child - isUserExists
                    ['child'],
                ],
                [], // child - isDatabaseExists
            );
        $connection
            ->expects($this->exactly(2))
            ->method('fetchFirstColumn')
            ->with($this->stringStartsWith('SELECT Child FROM'))
            ->willReturnOnConsecutiveCalls(
                [ // parent
                    'child',
                ],
                [], // child
            ); // getChildDatabases
        $connection
            ->expects($this->exactly(8))
            ->method('executeStatement')
            ->withConsecutive(
                // parent
                [<<<SQL
                    GRANT ALL ON "parent" TO "dbc"
                    SQL],
                [<<<SQL
                    GRANT DROP USER, DROP DATABASE ON "parent" TO "dbc"
                    SQL],
                [<<<SQL
                    DELETE USER "parent" ALL
                    SQL],
                // child
                [<<<SQL
                    GRANT ALL ON "child" TO "dbc"
                    SQL],
                [<<<SQL
                    GRANT DROP USER, DROP DATABASE ON "child" TO "dbc"
                    SQL],
                [<<<SQL
                    DELETE USER "child" ALL
                    SQL],
                [<<<SQL
                    DROP USER "child"
                    SQL],
                // back to parent
                [<<<SQL
                    DROP USER "parent"
                    SQL],
            );

        DbUtils::cleanUserOrDatabase($connection, 'parent', 'dbc');
    }

    public function testIsUserExists(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('fetchAllAssociative')
            ->with(<<<SQL
                SELECT * FROM DBC.DatabasesV t WHERE t.DatabaseName = 'db' AND t.DBKind = 'U'
                SQL)
            ->willReturn([
                ['some', 'data'],
            ]);

        $this->assertTrue(
            DbUtils::isUserExists($connection, 'db'),
        );
    }

    public function testIsDatabaseExists(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('fetchAllAssociative')
            ->with(<<<SQL
                SELECT * FROM DBC.DatabasesV t WHERE t.DatabaseName = 'db' AND t.DBKind = 'D'
                SQL)
            ->willReturn([
                ['some', 'data'],
            ]);

        $this->assertTrue(
            DbUtils::isDatabaseExists($connection, 'db'),
        );
    }

    public function testGetChildDatabases(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('fetchFirstColumn')
            ->with(<<<SQL
                SELECT Child FROM DBC.ChildrenV WHERE Parent = 'parent';
                SQL)
            ->willReturn([
                'first_child',
                'second_child',
            ]);

        $this->assertSame(
            [
                'first_child',
                'second_child',
            ],
            DbUtils::getChildDatabases($connection, 'parent'),
        );
    }

    public function testIsChildOf(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('fetchAllAssociative')
            ->with(<<<SQL
                SELECT * FROM DBC.ChildrenV WHERE Child = 'child' AND Parent = 'parent';
                SQL)
            ->willReturn([
                ['some', 'data'],
            ]);

        $this->assertTrue(
            DbUtils::isChildOf($connection, 'child', 'parent'),
        );
    }

    public function testDropRole(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('fetchAllAssociative')
            ->with($this->stringStartsWith('SELECT RoleName'))
            ->willReturn([
                ['role'],
            ]);
        $connection
            ->expects($this->once())
            ->method('executeQuery')
            ->with(<<<SQL
                DROP ROLE "role"
                SQL)
            ->willReturn($this->createMock(Result::class));

        DbUtils::dropRole($connection, 'role');
    }

    public function testIsRoleExists(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('fetchAllAssociative')
            ->with(<<<SQL
                SELECT RoleName FROM DBC.RoleInfoV WHERE RoleName = 'role'
                SQL)
            ->willReturn([
                ['role'],
            ]);

        $this->assertTrue(
            DbUtils::isRoleExists($connection, 'role'),
        );
    }

    public function testIsTableExists(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->once())
            ->method('fetchAllAssociative')
            ->with(<<<SQL
                SELECT TableName FROM DBC.TablesVX WHERE DatabaseName = 'database' AND TableName = 'table'
                SQL)
            ->willReturn([
                ['table'],
            ]);

        $this->assertTrue(
            DbUtils::isTableExists($connection, 'database', 'table'),
        );
    }
}
