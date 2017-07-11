<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer;

use Neos\Flow\Annotations as Flow;

/**
 * LoggerTrait
 */
trait LoggerTrait
{
    /**
     * @var \Neos\Flow\Log\SystemLoggerInterface
     * @Flow\Inject
     */
    protected $_logger;

    /**
     * @param string $message The message to log
     * @param integer $severity An integer value, one of the LOG_* constants
     * @param mixed $additionalData A variable containing more information about the event to be logged
     * @param string $packageKey Key of the package triggering the log (determined automatically if not specified)
     * @param string $className Name of the class triggering the log (determined automatically if not specified)
     * @param string $methodName Name of the method triggering the log (determined automatically if not specified)
     * @return void
     * @api
     */
    protected function log($message, $severity = LOG_INFO, $additionalData = null, $packageKey = null, $className = null, $methodName = null)
    {
        $packageKey = $packageKey ?: 'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer';
        $this->_logger->log($message, $severity, $additionalData, $packageKey, $className, $methodName);
    }
}
