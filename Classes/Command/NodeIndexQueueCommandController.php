<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Command;

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\NodeIndexer;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Mapping\NodeTypeMappingBuilder;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Domain\Repository\NodeDataRepository;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\IndexingJob;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\LoggerTrait;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\UpdateAliasJob;
use Flowpack\ElasticSearch\Domain\Model\Mapping;
use Flowpack\JobQueue\Common\Exception;
use Flowpack\JobQueue\Common\Job\JobManager;
use Flowpack\JobQueue\Common\Queue\QueueManager;
use Neos\ContentRepository\Domain\Repository\WorkspaceRepository;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use Neos\Utility\Files;

/**
 * Provides CLI features for index handling
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexQueueCommandController extends CommandController
{
    use LoggerTrait;

    const BATCH_QUEUE_NAME = 'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer';
    const LIVE_QUEUE_NAME = 'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer.Live';
    const DEFAULT_BATCH_SIZE = 500;

    /**
     * @var JobManager
     * @Flow\Inject
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
     * @var NodeTypeMappingBuilder
     * @Flow\Inject
     */
    protected $nodeTypeMappingBuilder;

    /**
     * @var NodeDataRepository
     * @Flow\Inject
     */
    protected $nodeDataRepository;

    /**
     * @var WorkspaceRepository
     * @Flow\Inject
     */
    protected $workspaceRepository;

    /**
     * @var NodeIndexer
     * @Flow\Inject
     */
    protected $nodeIndexer;

    /**
     * @Flow\InjectConfiguration(package="Flowpack.ElasticSearch.ContentRepositoryQueueIndexer")
     * @var array
     */
    protected $settings;

    /**
     * Index all nodes by creating a new index and when everything was completed, switch the index alias.
     *
     * @param string $workspace
     * @throws \Flowpack\JobQueue\Common\Exception
     * @throws \Neos\Flow\Mvc\Exception\StopActionException
     */
    public function buildCommand($workspace = null)
    {
        $indexPostfix = time();
        $indexName = $this->createNextIndex($indexPostfix);
        $this->updateMapping();

        $this->outputLine();
        $this->outputLine('<b>Indexing on %s ...</b>', [$indexName]);

        $pendingJobs = $this->queueManager->getQueue(self::BATCH_QUEUE_NAME)->count();
        if ($pendingJobs !== 0) {
            $this->outputLine('<error>!! </error> The queue "%s" is not empty (%d pending jobs), please flush the queue.', [self::BATCH_QUEUE_NAME, $pendingJobs]);
            $this->quit(1);
        }

        if ($workspace === null) {
            foreach ($this->workspaceRepository->findAll() as $workspace) {
                $workspace = $workspace->getName();
                $this->outputLine();
                $this->indexWorkspace($workspace, $indexPostfix);
            }
        } else {
            $this->outputLine();
            $this->indexWorkspace($workspace, $indexPostfix);
        }
        $updateAliasJob = new UpdateAliasJob($indexPostfix);
        $this->jobManager->queue(self::BATCH_QUEUE_NAME, $updateAliasJob);

        $this->outputLine("Indexing jobs created for queue %s with success ...", [self::BATCH_QUEUE_NAME]);
        $this->outputSystemReport();
        $this->outputLine();
    }

    /**
     * @param string $queue Type of queue to process, can be "live" or "batch"
     * @param int $exitAfter If set, this command will exit after the given amount of seconds
     * @param int $limit If set, only the given amount of jobs are processed (successful or not) before the script exits
     * @param bool $verbose Output debugging information
     * @return void
     * @throws \Neos\Flow\Mvc\Exception\StopActionException
     */
    public function workCommand($queue = 'batch', $exitAfter = null, $limit = null, $verbose = false)
    {
        $allowedQueues = [
            'batch' => self::BATCH_QUEUE_NAME,
            'live' => self::LIVE_QUEUE_NAME
        ];
        if (!isset($allowedQueues[$queue])) {
            $this->output('Invalid queue, should be "live" or "batch"');
        }
        $queueName = $allowedQueues[$queue];

        if ($verbose) {
            $this->output('Watching queue <b>"%s"</b>', [$queueName]);
            if ($exitAfter !== null) {
                $this->output(' for <b>%d</b> seconds', [$exitAfter]);
            }
            $this->outputLine('...');
        }

        $startTime = time();
        $timeout = null;
        $numberOfJobExecutions = 0;

        do {
            $message = null;
            if ($exitAfter !== null) {
                $timeout = max(1, $exitAfter - (time() - $startTime));
            }
            try {
                $message = $this->jobManager->waitAndExecute($queueName, $timeout);
            } catch (Exception $exception) {
                $numberOfJobExecutions++;
                $this->outputLine('<error>%s</error>', [$exception->getMessage()]);
                if ($verbose && $exception->getPrevious() instanceof \Exception) {
                    $this->outputLine('  Reason: %s', [$exception->getPrevious()->getMessage()]);
                }
            } catch (\Exception $exception) {
                $this->outputLine('<error>Unexpected exception during job execution: %s, aborting...</error>', [$exception->getMessage()]);
                $this->quit(1);
            }
            if ($message !== null) {
                $numberOfJobExecutions++;
                if ($verbose) {
                    $messagePayload = strlen($message->getPayload()) <= 50 ? $message->getPayload() : substr($message->getPayload(), 0, 50) . '...';
                    $this->outputLine('<success>Successfully executed job "%s" (%s)</success>', [$message->getIdentifier(), $messagePayload]);
                }
            }
            if ($exitAfter !== null && (time() - $startTime) >= $exitAfter) {
                if ($verbose) {
                    $this->outputLine('Quitting after %d seconds due to <i>--exit-after</i> flag', [time() - $startTime]);
                }
                $this->quit();
            }
            if ($limit !== null && $numberOfJobExecutions >= $limit) {
                if ($verbose) {
                    $this->outputLine('Quitting after %d executed job%s due to <i>--limit</i> flag', [$numberOfJobExecutions, $numberOfJobExecutions > 1 ? 's' : '']);
                }
                $this->quit();
            }
        } while (true);
    }

    /**
     * Flush the index queue
     */
    public function flushCommand()
    {
        try {
            $this->queueManager->getQueue(self::BATCH_QUEUE_NAME)->flush();
            $this->outputSystemReport();
        } catch (Exception $exception) {
            $this->outputLine('An error occurred: %s', [$exception->getMessage()]);
        }
        $this->outputLine();
    }

    /**
     * Output system report for CLI commands
     */
    protected function outputSystemReport()
    {
        $this->outputLine();
        $this->outputLine('Memory Usage   : %s', [Files::bytesToSizeString(memory_get_peak_usage(true))]);
        $time = microtime(true) - $_SERVER["REQUEST_TIME_FLOAT"];
        $this->outputLine('Execution time : %s seconds', [$time]);
        $this->outputLine('Indexing Queue : %s', [self::BATCH_QUEUE_NAME]);
        try {
            $this->outputLine('Pending Jobs   : %s', [$this->queueManager->getQueue(self::BATCH_QUEUE_NAME)->count()]);
        } catch (Exception $exception) {
            $this->outputLine('Pending Jobs   : Error, queue not found, %s', [$exception->getMessage()]);
        }
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
        $batchSize = $this->settings['batchSize'] ?? static::DEFAULT_BATCH_SIZE;
        while (true) {
            $iterator = $this->nodeDataRepository->findAllBySiteAndWorkspace($workspaceName, $offset, $batchSize);

            $jobData = [];

            foreach ($this->nodeDataRepository->iterate($iterator) as $data) {
                $jobData[] = [
                    'nodeIdentifier' => $data['nodeIdentifier'],
                    'dimensions' => $data['dimensions'],
                    'workspace' => $workspaceName,
                    'nodeType' => $data['nodeType'],
                    'path' => $data['path'],
                ];
                $nodeCounter++;
            }

            if ($jobData === []) {
                break;
            }

            $indexingJob = new IndexingJob($indexPostfix, $workspaceName, $jobData);
            $this->jobManager->queue(self::BATCH_QUEUE_NAME, $indexingJob);
            $this->output('.');
            $offset += $batchSize;
            $this->persistenceManager->clearState();
        }
        $this->outputLine();
        $this->outputLine("\nNumber of Nodes to be indexed in workspace '%s': %d", [$workspaceName, $nodeCounter]);
        $this->outputLine();
    }

    /**
     * @param string $indexPostfix
     * @return string
     */
    protected function createNextIndex($indexPostfix)
    {
        $this->nodeIndexer->setIndexNamePostfix($indexPostfix);
        $this->nodeIndexer->getIndex()->create();
        $this->log(sprintf('action=indexing step=index-created index=%s', $this->nodeIndexer->getIndexName()), LOG_INFO);

        return $this->nodeIndexer->getIndexName();
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
