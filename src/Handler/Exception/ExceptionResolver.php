<?php

namespace Keboola\StorageDriver\Teradata\Handler\Exception;

use Doctrine\DBAL\Exception as DBALException;
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
            {
                if (strpos($e->getMessage(), 'PERMANENT space is invalid') !== false) {
                    return NoSpaceException::createForFullParent($e);
                }
            }
        }

        return $e;
    }
}
