<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Indexer;

use Flowpack\ElasticSearch\ContentRepositoryAdaptor;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Command\NodeIndexQueueCommandController;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\IndexingJob;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\RemovalJob;
use Flowpack\JobQueue\Common\Job\JobManager;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use Neos\ContentRepository\Domain\Model\NodeInterface;

/**
 * ElasticSearch Indexing Job Interface
 */
class NodeIndexer extends ContentRepositoryAdaptor\Indexer\NodeIndexer
{
    /**
     * @var JobManager
     * @Flow\Inject
     */
    protected $jobManager;

    /**
     * @var PersistenceManagerInterface
     * @Flow\Inject
     */
    protected $persistenceManager;

    /**
     * @var bool
     * @Flow\InjectConfiguration(path="enableLiveAsyncIndexing")
     */
    protected $enableLiveAsyncIndexing;

    /**
     * @param NodeInterface $node
     * @param string|null $targetWorkspaceName In case indexing is triggered during publishing, a target workspace name will be passed in
     */
    public function indexNode(NodeInterface $node, $targetWorkspaceName = null)
    {
        if ($this->enableLiveAsyncIndexing !== true) {
            parent::indexNode($node, $targetWorkspaceName);
            return;
        }
        $indexingJob = new IndexingJob($this->indexNamePostfix, $targetWorkspaceName, [
            [
                'nodeIdentifier' => $this->persistenceManager->getIdentifierByObject($node->getNodeData()),
                'dimensions' => $node->getDimensions(),
                'workspace' => $node->getWorkspace()->getName(),
                'nodeType' => $node->getNodeType()->getName(),
                'path' => $node->getPath(),
            ]
        ]);
        $this->jobManager->queue(NodeIndexQueueCommandController::LIVE_QUEUE_NAME, $indexingJob);
    }

    /**
     * @param NodeInterface $node
     * @param string|null $targetWorkspaceName In case indexing is triggered during publishing, a target workspace name will be passed in
     */
    public function removeNode(NodeInterface $node, $targetWorkspaceName = null)
    {
        if ($this->enableLiveAsyncIndexing !== true) {
            parent::removeNode($node, $targetWorkspaceName);
            return;
        }
        $removalJob = new RemovalJob($this->indexNamePostfix, $targetWorkspaceName, [
            [
                'nodeIdentifier' => $this->persistenceManager->getIdentifierByObject($node->getNodeData()),
                'dimensions' => $node->getDimensions(),
                'workspace' => $node->getWorkspace()->getName(),
                'nodeType' => $node->getNodeType()->getName(),
                'path' => $node->getPath(),
            ]
        ]);
        $this->jobManager->queue(NodeIndexQueueCommandController::LIVE_QUEUE_NAME, $removalJob);
    }
}
