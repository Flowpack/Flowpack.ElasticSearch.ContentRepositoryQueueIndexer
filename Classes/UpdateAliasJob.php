<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentRepositoryQueueIndexer package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\NodeIndexer;
use Flowpack\ElasticSearch\Transfer\Exception\ApiException;
use Flowpack\JobQueue\Common\Job\JobInterface;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Utility\Algorithms;

class UpdateAliasJob implements JobInterface
{
    use LoggerTrait;

    /**
     * @var NodeIndexer
     * @Flow\Inject
     */
    protected $nodeIndexer;

    /**
     * @var string
     */
    protected $identifier;

    /**
     * @var string
     */
    protected $indexPostfix;

    /**
     * @Flow\InjectConfiguration(path="acceptedFailedJobs")
     * @var int
     */
    protected $acceptedFailedJobs = -1;

    /**
     * @Flow\InjectConfiguration(path="cleanupIndicesAfterSuccessfulSwitch")
     * @var bool
     */
    protected $cleanupIndicesAfterSuccessfulSwitch = true;


    /**
     * @param string $indexPostfix
     */
    public function __construct($indexPostfix)
    {
        $this->identifier = Algorithms::generateRandomString(24);
        $this->indexPostfix = $indexPostfix;
    }

    /**
     * @param QueueInterface $queue
     * @param Message $message The original message
     * @return boolean TRUE if the job was executed successfully and the message should be finished
     * @throws \Exception
     * @throws \Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception
     * @throws \Flowpack\ElasticSearch\Transfer\Exception\ApiException
     */
    public function execute(QueueInterface $queue, Message $message): bool
    {
        if ($this->shouldIndexBeSwitched($queue)) {
            $this->nodeIndexer->setIndexNamePostfix($this->indexPostfix);
            $this->nodeIndexer->updateIndexAlias();

            if ($this->cleanupIndicesAfterSuccessfulSwitch === true) {
                $this->cleanupOldIndices();
            }

            $this->log(sprintf('action=indexing step=index-switched alias=%s message="Index was switched successfully"', $this->indexPostfix), LOG_NOTICE);
        } else {
            $this->log(sprintf('action=indexing step=index-switched alias=%s message="Index was not switched due to %s failed batches in the current queue"', $this->indexPostfix, $queue->countFailed()), LOG_ERR);
        }

        return true;
    }

    /**
     * Get an optional identifier for the job
     *
     * @return string A job identifier
     */
    public function getIdentifier()
    {
        return $this->identifier;
    }

    /**
     * Get a readable label for the job
     *
     * @return string A label for the job
     */
    public function getLabel(): string
    {
        return sprintf('ElasticSearch Indexing Job (%s)', $this->getIdentifier());
    }

    /**
     * @throws \Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception
     */
    protected function cleanupOldIndices()
    {
        try {
            $indicesToBeRemoved = $this->nodeIndexer->removeOldIndices();
            if (count($indicesToBeRemoved) > 0) {
                foreach ($indicesToBeRemoved as $indexToBeRemoved) {
                    $this->log(sprintf('action=indexing step=index-switched alias=%s message="Old index was successfully removed"', $indexToBeRemoved), LOG_INFO);
                }
            }
        } catch (ApiException $exception) {
            $response = json_decode($exception->getResponse());
            if ($response->error instanceof \stdClass) {
                $this->log(sprintf('action=indexing step=index-switched alias=%s message="Old indices could not be removed. ElasticSearch responded with status %s, saying "%s: %s"', $this->indexPostfix, $response->status, $response->error->type, $response->error->reason), LOG_ERR);
            } else {
                $this->log(sprintf('action=indexing step=index-switched alias=%s message="Old indices could not be removed. ElasticSearch responded with status %s, saying "%s"', $this->indexPostfix, $response->status, $response->error), LOG_ERR);
            }
        }
    }

    /**
     * @param QueueInterface $queue
     * @return bool
     */
    protected function shouldIndexBeSwitched(QueueInterface $queue): bool
    {
        if ($this->acceptedFailedJobs === -1) {
            return true;
        }

        if ($queue->countFailed() <= $this->acceptedFailedJobs) {
            return true;
        }

        return false;
    }
}
