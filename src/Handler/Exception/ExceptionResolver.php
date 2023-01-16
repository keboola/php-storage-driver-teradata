<?php

namespace Keboola\StorageDriver\Teradata\Handler\Exception;

use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Exception\DriverException;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\Exception\NoMoreRoomInTDException;
use ReflectionClass;

class ExceptionResolver
{
    public static function resolveException(\Throwable $e)
    {
        $exceptionClass = new ReflectionClass($e);

        switch ($exceptionClass->getName()) {
            case NoMoreRoomInTDException::class:
                return NoSpaceException::createForFullDB($e);
            case DBALException::class:
            case DriverException::class:
            {
                if (strpos($e->getMessage(), 'PERMANENT space is invalid') !== false) {
                    return NoSpaceException::createForFullParent($e);
                }
                if (strpos($e->getMessage(), 'No more room in database') !== false) {
                    return NoSpaceException::createForFullDB($e);
                }
            }
        }

        return $e;
    }
}
