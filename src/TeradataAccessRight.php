<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Teradata;

final class TeradataAccessRight
{
    public const RIGHT_ALTER_EXTERNAL_PROCEDURE = 'AE'; // Alter External Procedure
    public const RIGHT_ALTER_FUNCTION = 'AF'; // Alter Function
    public const RIGHT_ALTER_PROCEDURE = 'AP'; // Alter Procedure
    public const RIGHT_ABORT_SESSION = 'AS'; // Abort Session
    public const RIGHT_CREATE_AUTHORIZATION = 'CA'; // Create Authorization
    public const RIGHT_CREATE_DATABASE = 'CD'; // Create Database
    public const RIGHT_CREATE_EXTERNAL_PROCEDURE = 'CE'; // Create External Procedure
    public const RIGHT_CREATE_FUNCTION = 'CF'; // Create Function
    public const RIGHT_CREATE_TRIGGER = 'CG'; // Create Trigger
    public const RIGHT_CREATE_MACRO = 'CM'; // Create Macro
    public const RIGHT_CREATE_PROFILE = 'CO'; // Create Profile
    public const RIGHT_CHECKPOINT = 'CP'; // Checkpoint
    public const RIGHT_CREATE_ROLE = 'CR'; // Create Role
    public const RIGHT_CREATE_TABLE = 'CT'; // Create Table
    public const RIGHT_CREATE_USER = 'CU'; // Create User
    public const RIGHT_CREATE_VIEW = 'CV'; // Create View
    public const RIGHT_DELETE = 'D'; // Delete
    public const RIGHT_DROP_AUTHORIZATION = 'DA'; // Drop Authorization
    public const RIGHT_DROP_DATABASE = 'DD'; // Drop Database
    public const RIGHT_DROP_FUNCTION = 'DF'; // Drop Function
    public const RIGHT_DROP_TRIGGER = 'DG'; // Drop Trigger
    public const RIGHT_DROP_MACRO = 'DM'; // Drop Macro
    public const RIGHT_DROP_PROFILE = 'DO'; // Drop Profile
    public const RIGHT_DUMP = 'DP'; // Dump
    public const RIGHT_DROP_ROLE = 'DR'; // Drop Role
    public const RIGHT_DROP_TABLE = 'DT'; // Drop Table
    public const RIGHT_DROP_USER = 'DU'; // Drop User
    public const RIGHT_DROP_VIEW = 'DV'; // Drop View
    public const RIGHT_EXECUTE = 'E'; // Execute
    public const RIGHT_EXECUTE_FUNCTION = 'EF'; // Execute Function
    public const RIGHT_CREATE_GLOP_SET = 'GC'; // Create GLOP SET
    public const RIGHT_DROP_GLOP_SET = 'GD'; // Drop GLOP SET
    public const RIGHT_GLOP_MEMBER = 'GM'; // GLOP Member
    public const RIGHT_INSERT = 'I'; // Insert
    public const RIGHT_INDEXES = 'IX'; // Indexes
    public const RIGHT_MONITOR_RESOURCE = 'MR'; // Monitor Resource
    public const RIGHT_MONITOR_SESSION = 'MS'; // Monitor Session
    public const RIGHT_NON_TEMPORAL = 'NT'; // Non Temporal
    public const RIGHT_OVERRIDE_ARCHIVE_CONSTRAINT = 'OA'; // Override Archive Constraint
    public const RIGHT_OVERRIDE_DELETE_CONSTRAINT = 'OD'; // Override Delete Constraint
    public const RIGHT_OVERRIDE_INSERT_CONSTRAINT = 'OI'; // Override Insert Constraint
    public const RIGHT_CREATE_OWNER_PROCEDURE = 'OP'; // Create Owner Procedure
    public const RIGHT_OVERRIDE_RESTORE_CONSTRAINT = 'OR'; // Override Restore Constraint
    public const RIGHT_OVERRIDE_SELECT_CONSTRAINT = 'OS'; // Override Select Constraint
    public const RIGHT_OVERRIDE_UPDATE_CONSTRAINT = 'OU'; // Override Update Constraint
    public const RIGHT_CREATE_PROCEDURE = 'PC'; // Create Procedure
    public const RIGHT_DROP_PROCEDURE = 'PD'; // Drop Procedure
    public const RIGHT_EXECUTE_PROCEDURE = 'PE'; // Execute Procedure
    public const RIGHT_RETRIEVE_OR_SELECT = 'R'; // Retrieve or Select
    public const RIGHT_REFERENCES = 'RF'; // References
    public const RIGHT_RESTORE = 'RS'; // Restore
    public const RIGHT_SHOW = 'SH'; // Show
    public const RIGHT_CONSTRAINT_ASSIGNMENT = 'SA'; // Constraint Assignment
    public const RIGHT_CONSTRAINT_DEFINITION = 'SD'; // Constraint Definition
    public const RIGHT_SET_SESSION_RATE = 'SS'; // Set Session Rate
    public const RIGHT_SET_RESOURCE_RATE = 'SR'; // Set Resource Rate
    public const RIGHT_STATISTICS = 'ST'; // Statistics
    public const RIGHT_CONNECT_THROUGH = 'TH'; // Connect Through
    public const RIGHT_UPDATE = 'U'; // Update
    public const RIGHT_UDT_METHOD = 'UM'; // UDT Method
    public const RIGHT_UDT_TYPE = 'UT'; // UDT Type
    public const RIGHT_UDT_USAGE = 'UU'; // UDT Usage
}
