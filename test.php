<?php

require __DIR__ . '/vendor/autoload.php';

\GPBMetadata\Proto\Backend::initOnce();
\GPBMetadata\Proto\Bucket::initOnce();
\GPBMetadata\Proto\Common::initOnce();
\GPBMetadata\Proto\Credentials::initOnce();
\GPBMetadata\Proto\Info::initOnce();
\GPBMetadata\Proto\Project::initOnce();
\GPBMetadata\Proto\Table::initOnce();
\GPBMetadata\Proto\Workspace::initOnce();

$json = '{"credentials":{"@type":"type.googleapis.com/keboola.storageDriver.credentials.GenericBackendCredentials","host":"xxx","principal":"xxx","secret":"xxx","meta":{"@type":"type.googleapis.com/keboola.storageDriver.credentials.GenericBackendCredentials.TeradataCredentialsMeta","database":"xxx"}},"command":{"@type":"type.googleapis.com/keboola.storageDriver.command.table.TableImportFromFileCommand","formatTypeOptions":{"@type":"type.googleapis.com/keboola.storageDriver.command.table.TableImportFromFileCommand.CsvTypeOptions","columnsNames":["id","name","city","sex"],"delimiter":",","enclosure":"\"","compression":"GZIP"},"filePath":{"root":"xxx","path":"xxx","fileName":"xxx"},"fileCredentials":{"@type":"type.googleapis.com/keboola.storageDriver.command.table.ImportExportShared.S3Credentials","key":"xxx","secret":"xxx","region":"eu-central-1"},"destination":{"path":["xxx"],"tableName":"xxx"},"importOptions":{"timestampColumn":"_timestamp","numberOfIgnoredLines":1,"dedupType":"INSERT_DUPLICATES"},"meta":{"@type":"type.googleapis.com/keboola.storageDriver.command.table.TableImportFromFileCommand.TeradataTableImportMeta"}}}';

$m = new \Keboola\StorageDriver\Command\Common\DriverRequest();

$m->mergeFromJsonString($json);
$json = $m->serializeToJsonString();
var_export($json);

echo PHP_EOL;
echo PHP_EOL;

$m = new \Keboola\StorageDriver\Command\Common\DriverRequest();

$m->mergeFromJsonString($json);
