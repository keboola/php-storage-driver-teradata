<?php

require __DIR__ . '/../../vendor/autoload.php';

set_error_handler(function($errno, $errstr, $errfile, $errline ) {
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
