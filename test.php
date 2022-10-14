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

$jsonNoFileCredentials = '{
    "credentials": {
        "@type": "type.googleapis.com/keboola.storageDriver.credentials.GenericBackendCredentials",
        "host": "xxx",
        "principal": "xxx",
        "secret": "xxx",
        "meta": {
            "@type": "type.googleapis.com/keboola.storageDriver.credentials.GenericBackendCredentials.TeradataCredentialsMeta",
            "database": "xxx"
        }
    },
    "command": {
        "@type": "type.googleapis.com/keboola.storageDriver.command.table.TableImportFromFileCommand",
        "filePath": {}
    }
}';

$m = new \Keboola\StorageDriver\Command\Common\DriverRequest();

$m->mergeFromJsonString($jsonNoFileCredentials);
$json = $m->serializeToJsonString();
var_export($json);

echo PHP_EOL;
echo PHP_EOL;

$m = new \Keboola\StorageDriver\Command\Common\DriverRequest();

$m->mergeFromJsonString($json);


echo PHP_EOL;
echo 'This is OK.';
echo PHP_EOL;
echo 'Now with fileCredentials type.';
echo PHP_EOL;

$json = '{
    "credentials": {
        "@type": "type.googleapis.com/keboola.storageDriver.credentials.GenericBackendCredentials",
        "host": "xxx",
        "principal": "xxx",
        "secret": "xxx",
        "meta": {
            "@type": "type.googleapis.com/keboola.storageDriver.credentials.GenericBackendCredentials.TeradataCredentialsMeta",
            "database": "xxx"
        }
    },
    "command": {
        "@type": "type.googleapis.com/keboola.storageDriver.command.table.TableImportFromFileCommand",
        
        "filePath": {},
        "fileCredentials": {
            "@type": "type.googleapis.com/keboola.storageDriver.command.table.ImportExportShared.S3Credentials"
        }
    }
}';

$m = new \Keboola\StorageDriver\Command\Common\DriverRequest();

$m->mergeFromJsonString($json);
$json = $m->serializeToJsonString();
var_export($json);

echo PHP_EOL;
echo PHP_EOL;

$m = new \Keboola\StorageDriver\Command\Common\DriverRequest();

$m->mergeFromJsonString($json);
