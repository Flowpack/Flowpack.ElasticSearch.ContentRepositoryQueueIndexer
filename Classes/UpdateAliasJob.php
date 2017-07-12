<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer;

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\NodeIndexer;
use Flowpack\JobQueue\Common\Job\JobInterface;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Utility\Algorithms;

/**
 * ElasticSearch Indexing Job Interface
 */
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
     * @param string $indexPostfix
     */
    public function __construct($indexPostfix)
    {
        $this->identifier = Algorithms::generateRandomString(24);
        $this->indexPostfix = $indexPostfix;
    }

    /**
     * Execute the job
     * A job should finish itself after successful execution using the queue methods.
     *
     * @param QueueInterface $queue
     * @param Message $message The original message
     * @return boolean TRUE if the job was executed successfully and the message should be finished
     */
    public function execute(QueueInterface $queue, Message $message)
    {
        $this->nodeIndexer->setIndexNamePostfix($this->indexPostfix);
        $this->nodeIndexer->updateIndexAlias();
        $this->log(sprintf('action=indexing step=index-switched alias=%s', $this->indexPostfix), LOG_NOTICE);

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
    public function getLabel()
    {
        return sprintf('ElasticSearch Indexing Job (%s)', $this->getIdentifier());
    }
}
