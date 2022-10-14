<?php

declare(strict_types=1);

use Keboola\StorageDriver\Command\Common\DriverRequest;
use Keboola\StorageDriver\Controller\Retry;
use Keboola\StorageDriver\Teradata\DriverTask;
use Spiral\RoadRunner\Environment;
use function Keboola\StorageDriver\Controller\retry;

require __DIR__ . '/../../vendor/autoload.php';

\GPBMetadata\Proto\Backend::initOnce();
\GPBMetadata\Proto\Bucket::initOnce();
\GPBMetadata\Proto\Common::initOnce();
\GPBMetadata\Proto\Credentials::initOnce();
\GPBMetadata\Proto\Info::initOnce();
\GPBMetadata\Proto\Project::initOnce();
\GPBMetadata\Proto\Table::initOnce();
\GPBMetadata\Proto\Workspace::initOnce();

$env = Environment::fromGlobals();
$rpc = \Spiral\Goridge\RPC\RPC::create($env->getRPCAddress());
$factory = new \Spiral\RoadRunner\KeyValue\Factory($rpc);

set_error_handler(function ($errno, $errstr, $errfile, $errline) {
    throw new ErrorException($errstr, $errno, 0, $errfile, $errline);
});

$storage = $factory->select('storage-driver');

/** @var array<string, string> $env */
$env = \array_merge($_ENV, $_SERVER);

$req = new DriverRequest();
$req->mergeFromJsonString($env['ACTIVITY_INPUT']);
$workflowId = $env['WORKFLOW_ID'];

$task = new DriverTask(
    $req,
    $workflowId
);

try {
    $res = $task->run();
    if ($res !== null) {
        Retry::retryUniform(fn()=>$storage->set($workflowId, json_encode(['response' => $res->serializeToJsonString()], JSON_THROW_ON_ERROR)));
    } else {
        Retry::retryUniform(fn()=>$storage->set($workflowId, '{}'));
    }
} catch (\Throwable $e) {
    Retry::retryUniform(fn()=>$storage->set($workflowId, json_encode(['exception' => $e], JSON_THROW_ON_ERROR)));
}

exit(0);
