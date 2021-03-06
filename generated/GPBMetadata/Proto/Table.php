<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/table.proto

namespace GPBMetadata\Proto;

class Table
{
    public static $is_initialized = false;

    public static function initOnce() {
        $pool = \Google\Protobuf\Internal\DescriptorPool::getGeneratedPool();

        if (static::$is_initialized == true) {
          return;
        }
        \GPBMetadata\Google\Protobuf\Any::initOnce();
        \GPBMetadata\Google\Protobuf\Struct::initOnce();
        \GPBMetadata\Proto\Info::initOnce();
        $pool->internalAddGeneratedFile(
            '
?-
proto/table.proto#keboola.storageDriver.command.tablegoogle/protobuf/struct.protoproto/info.proto"?
CreateTableCommand
path (	
	tableName (	T
columns (2C.keboola.storageDriver.command.table.CreateTableCommand.TableColumn
primaryKeysNames (	"
meta (2.google.protobuf.Any?
TableColumn
name (	
type (	
length (	
nullable (
default (	"
meta (2.google.protobuf.Any*
TeradataTableColumnMeta
isLatin (
SynapseTableMeta"I
DropTableCommand
path (	
	tableName (	
ignoreErrors ("
AlterTableCommand"?
PreviewTableCommand
path (	
	tableName (	
limit (
changeSince (	
changeUntil (	
columns (	
fulltextSearch (	^
whereFilters (2H.keboola.storageDriver.command.table.ImportExportShared.TableWhereFilter]
orderBy	 (2L.keboola.storageDriver.command.table.PreviewTableCommand.PreviewTableOrderBy?
PreviewTableOrderBy

columnName (	a
order (2R.keboola.storageDriver.command.table.PreviewTableCommand.PreviewTableOrderBy.OrderR
dataType (2@.keboola.storageDriver.command.table.ImportExportShared.DataType"
Order
ASC 
DESC"?
PreviewTableResponse
columns (	K
rows (2=.keboola.storageDriver.command.table.PreviewTableResponse.Row?
RowU
columns (2D.keboola.storageDriver.command.table.PreviewTableResponse.Row.ColumnX
Column

columnName (	%
value (2.google.protobuf.Value
isTruncated ("?	
ImportExportShared?
TableWhereFilter
columnsName (	c
operator (2Q.keboola.storageDriver.command.table.ImportExportShared.TableWhereFilter.Operator
values (	R
dataType (2@.keboola.storageDriver.command.table.ImportExportShared.DataType":
Operator
eq 
ne
gt
ge
lt
le(
Table
path (	
	tableName (	?
ImportOptions
timestampColumn (	)
!convertEmptyValuesToNullOnColumns (	d

importType (2P.keboola.storageDriver.command.table.ImportExportShared.ImportOptions.ImportType
numberOfIgnoredLines (b
	dedupType (2O.keboola.storageDriver.command.table.ImportExportShared.ImportOptions.DedupType
dedupColumnsNames (	"\'

ImportType
FULL 
INCREMENTAL"Q
	DedupType
UPDATE_DUPLICATES 
INSERT_DUPLICATES
FAIL_ON_DUPLICATES>
ExportOptions
isCompressed (
columnsToExport (	K
S3Credentials
key (	
secret (	
region (	
token (	K
ABSCredentials
accountName (	
sasToken (	

accountKey (	8
FilePath
root (	
path (	
fileName (	"R
DataType

STRING 
INTEGER

DOUBLE

BIGINT
REAL
DECIMAL"
FileProvider
S3 
ABS"

FileFormat
CSV "?	
TableImportFromFileCommandZ
fileProvider (2D.keboola.storageDriver.command.table.ImportExportShared.FileProviderV

fileFormat (2B.keboola.storageDriver.command.table.ImportExportShared.FileFormat/
formatTypeOptions (2.google.protobuf.AnyR
filePath (2@.keboola.storageDriver.command.table.ImportExportShared.FilePath-
fileCredentials (2.google.protobuf.AnyR
destination (2=.keboola.storageDriver.command.table.ImportExportShared.Table\\
importOptions (2E.keboola.storageDriver.command.table.ImportExportShared.ImportOptions"
meta (2.google.protobuf.Any?
CsvTypeOptions
columnsNames (	
	delimiter (	
	enclosure (	
	escapedBy (	m

sourceType (2Y.keboola.storageDriver.command.table.TableImportFromFileCommand.CsvTypeOptions.SourceTypeo
compression (2Z.keboola.storageDriver.command.table.TableImportFromFileCommand.CsvTypeOptions.Compression"=

SourceType
SINGLE_FILE 
SLICED_FILE
	DIRECTORY"!
Compression
NONE 
GZIP?
TeradataTableImportMeta|
importAdapter (2e.keboola.storageDriver.command.table.TableImportFromFileCommand.TeradataTableImportMeta.ImportAdapter"
ImportAdapter
TPT "?
TableImportResponse
importedRowsCount (
tableRowsCount (
tableSizeBytes (N
timers (2>.keboola.storageDriver.command.table.TableImportResponse.Timer
importedColumns (	"
meta (2.google.protobuf.Any\'
Timer
name (	
duration (	d
TeradataTableImportMeta
	importLog (	
errorTable1records (	
errorTable2records (	"?
TableImportFromTableCommandc
source (2S.keboola.storageDriver.command.table.TableImportFromTableCommand.SourceTableMappingR
destination (2=.keboola.storageDriver.command.table.ImportExportShared.Table\\
importOptions (2E.keboola.storageDriver.command.table.ImportExportShared.ImportOptions?
SourceTableMapping
path (	
	tableName (	
seconds (^
whereFilters (2H.keboola.storageDriver.command.table.ImportExportShared.TableWhereFilter
limit (y
columnMappings (2a.keboola.storageDriver.command.table.TableImportFromTableCommand.SourceTableMapping.ColumnMappingH
ColumnMapping
sourceColumnName (	
destinationColumnName (	"?
TableExportToFileCommandM
source (2=.keboola.storageDriver.command.table.ImportExportShared.TableZ
fileProvider (2D.keboola.storageDriver.command.table.ImportExportShared.FileProviderV

fileFormat (2B.keboola.storageDriver.command.table.ImportExportShared.FileFormatR
filePath (2@.keboola.storageDriver.command.table.ImportExportShared.FilePath-
fileCredentials (2.google.protobuf.Any\\
exportOptions (2E.keboola.storageDriver.command.table.ImportExportShared.ExportOptions"
meta (2.google.protobuf.Any?
TeradataTableExportMetaz
exportAdapter (2c.keboola.storageDriver.command.table.TableExportToFileCommand.TeradataTableExportMeta.ExportAdapter"
ExportAdapter
TPT "]
TableExportToFileResponse@
	tableInfo (2-.keboola.storageDriver.command.info.TableInfobproto3'
        , true);

        static::$is_initialized = true;
    }
}

