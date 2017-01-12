<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Command;

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\NodeIndexer;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Mapping\NodeTypeMappingBuilder;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Domain\Repository\NodeDataRepository;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\IndexingJob;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\LoggerTrait;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\UpdateAliasJob;
use Flowpack\ElasticSearch\Domain\Model\Mapping;
use Flowpack\JobQueue\Common\Job\JobManager;
use Flowpack\JobQueue\Common\Queue\QueueManager;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Cli\CommandController;
use TYPO3\Flow\Persistence\PersistenceManagerInterface;
use TYPO3\Flow\Utility\Files;
use TYPO3\TYPO3CR\Domain\Repository\WorkspaceRepository;

/**
 * Provides CLI features for index handling
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexQueueCommandController extends CommandController
{
    use LoggerTrait;

    const QUEUE_NAME = 'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer';

    /**
     * @Flow\Inject
     * @var JobManager
     */
    protected $jobManager;

    /**
     * @var QueueManager
     * @Flow\Inject
     */
    protected $queueManager;

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
     * Flush the index queue
     */
    public function flushCommand()
    {
        $this->queueManager->getQueue(self::QUEUE_NAME)->flush();
        $this->outputSystemReport();
        $this->outputLine();
    }

    protected function outputSystemReport()
    {
        $this->outputLine();
        $this->outputLine('Memory Usage   : %s', [Files::bytesToSizeString(memory_get_peak_usage(true))]);
        $time = microtime(true) - $_SERVER["REQUEST_TIME_FLOAT"];
        $this->outputLine('Execution time : %s seconds', [$time]);
        $this->outputLine('Indexing Queue : %s', [self::QUEUE_NAME]);
        $this->outputLine('Pending Jobs   : %s', [$this->queueManager->getQueue(self::QUEUE_NAME)->count()]);
    }

    /**
     * @param string $workspaceName
     * @param string $indexPostfix
     */
    protected function indexWorkspace($workspaceName, $indexPostfix)
    {
        $this->outputLine('<info>++</info> Indexing %s workspace', [$workspaceName]);
        $nodeCounter = 0;
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
                $nodeCounter++;
            }

            if ($jobData === []) {
                break;
            }

            $indexingJob = new IndexingJob($indexPostfix, $workspaceName, $jobData);
            $this->jobManager->queue(self::QUEUE_NAME, $indexingJob);
            $this->output('.');
            $offset += $batchSize;
            $this->persistenceManager->clearState();
        }
        $this->outputLine();
        $this->outputLine("\nNumber of Nodes be indexed in workspace '%s': %d", [$workspaceName, $nodeCounter]);
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
        $this->log(sprintf('action=indexing step=index-created index=%s', $this->nodeIndexer->getIndexName()), LOG_INFO);
    }

    /**
     * Update Index Mapping
     */
    protected function updateMapping()
    {
        $nodeTypeMappingCollection = $this->nodeTypeMappingBuilder->buildMappingInformation($this->nodeIndexer->getIndex());
        foreach ($nodeTypeMappingCollection as $mapping) {
            /** @var Mapping $mapping */
            $mapping->apply();
        }
        $this->log(sprintf('action=indexing step=mapping-updated index=%s', $this->nodeIndexer->getIndexName()), LOG_INFO);
    }
}
