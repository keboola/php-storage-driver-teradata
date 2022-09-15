<?php

require __DIR__ . '/../../vendor/autoload.php';

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
    'driver-controller'
);

$worker->registerWorkflowTypes(
    \Keboola\StorageDriver\Teradata\Temporal\ControllerExecutorWorkflow::class,
    \Keboola\StorageDriver\Teradata\Temporal\ControllerKillWorkflow::class
);

// start primary loop
$factory->run();
