<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Command;

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\NodeIndexer;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\LoggerInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Mapping\NodeTypeMappingBuilder;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Domain\Repository\NodeDataRepository;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\IndexingJob;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\UpdateAliasJob;
use Flowpack\JobQueue\Common\Job\JobManager;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Cli\CommandController;
use TYPO3\Flow\Persistence\PersistenceManagerInterface;
use TYPO3\TYPO3CR\Domain\Repository\WorkspaceRepository;

/**
 * Provides CLI features for index handling
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexQueueCommandController extends CommandController
{
    /**
     * @Flow\Inject
     * @var JobManager
     */
    protected $jobManager;

    /**
     * @var PersistenceManagerInterface
     * @Flow\Inject
     */
    protected $persistenceManager;

    /**
     * @Flow\Inject
     * @var NodeTypeMappingBuilder
     */
    protected $nodeTypeMappingBuilder;

    /**
     * @Flow\Inject
     * @var NodeDataRepository
     */
    protected $nodeDataRepository;

    /**
     * @Flow\Inject
     * @var WorkspaceRepository
     */
    protected $workspaceRepository;

    /**
     * @Flow\Inject
     * @var NodeIndexer
     */
    protected $nodeIndexer;

    /**
     * @Flow\Inject
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * Index all nodes by creating a new index and when everything was completed, switch the index alias.
     *
     * @param string $workspace
     */
    public function buildCommand($workspace = null)
    {
        $indexPostfix = time();
        $this->createNextIndex($indexPostfix);
        $this->updateMapping();


        $this->outputLine(sprintf('Indexing on %s ... ', $indexPostfix));

        if ($workspace === null) {
            foreach ($this->workspaceRepository->findAll() as $workspace) {
                $workspace = $workspace->getName();
                $this->outputLine();
                $this->outputLine(sprintf('<info>++</info> Indexing %s workspace', $workspace));
                $this->indexWorkspace($workspace, $indexPostfix);
            }
        } else {
            $this->outputLine();
            $this->outputLine(sprintf('<info>++</info> Indexing only %s workspace', $workspace));
            $this->indexWorkspace($workspace, $indexPostfix);
        }
        $updateAliasJob = new UpdateAliasJob($indexPostfix);
        $queueName = 'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer';
        $this->jobManager->queue($queueName, $updateAliasJob);

        $this->outputLine();
        $this->outputLine(sprintf('Indexing jobs created for queue %s with success ...', $queueName));
    }

    /**
     * @param string $workspaceName
     * @param string $indexPostfix
     */
    protected function indexWorkspace($workspaceName, $indexPostfix)
    {
        $offset = 0;
        $batchSize = 250;
        while (true) {
            $iterator = $this->nodeDataRepository->findAllBySiteAndWorkspace($workspaceName, $offset, $batchSize);

            $jobData = [];

            foreach ($this->nodeDataRepository->iterate($iterator) as $data) {
                $jobData[] = [
                    'nodeIdentifier' => $data['nodeIdentifier'],
                    'dimensions' => $data['dimensions']
                ];
            }

            if ($jobData === []) {
                break;
            }

            $indexingJob = new IndexingJob($indexPostfix, $workspaceName, $jobData);
            $this->jobManager->queue('Flowpack.ElasticSearch.ContentRepositoryQueueIndexer', $indexingJob);
            $this->output('.');
            $offset += $batchSize;
            $this->persistenceManager->clearState();
        }
        $this->outputLine();
    }

    /**
     * Create next index
     * @param string $indexPostfix
     */
    protected function createNextIndex($indexPostfix)
    {
        $this->nodeIndexer->setIndexNamePostfix($indexPostfix);
        $this->nodeIndexer->getIndex()->create();
        $this->logger->log(sprintf('action=indexing step=index-created index=%s', $this->nodeIndexer->getIndexName()), LOG_INFO);
    }

    /**
     * Update Index Mapping
     */
    protected function updateMapping()
    {
        $nodeTypeMappingCollection = $this->nodeTypeMappingBuilder->buildMappingInformation($this->nodeIndexer->getIndex());
        foreach ($nodeTypeMappingCollection as $mapping) {
            /** @var \Flowpack\ElasticSearch\Domain\Model\Mapping $mapping */
            $mapping->apply();
        }
        $this->logger->log(sprintf('action=indexing step=mapping-updated index=%s', $this->nodeIndexer->getIndexName()), LOG_INFO);
    }
}
