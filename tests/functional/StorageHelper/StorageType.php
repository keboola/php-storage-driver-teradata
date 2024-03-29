<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\StorageHelper;

class StorageType
{
    public const STORAGE_S3 = 'S3';
    public const STORAGE_ABS = 'ABS';

    public const STORAGES = [
        self::STORAGE_S3,
        self::STORAGE_ABS,
    ];
}
