<?php

declare(strict_types=1);

require __DIR__ . '/../../vendor/autoload.php';

set_error_handler(function ($errno, $errstr, $errfile, $errline) {
    throw new ErrorException($errstr, $errno, 0, $errfile, $errline);
});

\GPBMetadata\Proto\Backend::initOnce();
\GPBMetadata\Proto\Bucket::initOnce();
\GPBMetadata\Proto\Common::initOnce();
\GPBMetadata\Proto\Credentials::initOnce();
\GPBMetadata\Proto\Info::initOnce();
\GPBMetadata\Proto\Project::initOnce();
\GPBMetadata\Proto\Table::initOnce();
\GPBMetadata\Proto\Workspace::initOnce();

// factory initiates and runs task queue specific activity and workflow workers
$factory = \Temporal\WorkerFactory::create();

// Worker that listens on a task queue and hosts both workflow and activity implementations.
$worker = $factory->newWorker(
    'driver-teradata'
);

$worker->registerActivityImplementations(
    new \Keboola\StorageDriver\Controller\DriverCommandActivity()
);

// start primary loop
$factory->run();
