<?php
declare(strict_types=1);

namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Command;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentRepositoryQueueIndexer package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\NodeTypeMappingBuilderInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception\ConfigurationException;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\NodeIndexer;
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
use Neos\Flow\Cli\Exception\StopCommandException;
use Neos\Flow\Log\Utility\LogEnvironment;
use Neos\Flow\Mvc\Exception\StopActionException;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use Neos\Utility\Files;
use Psr\Log\LoggerInterface;

/**
 * Provides CLI features for index handling
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexQueueCommandController extends CommandController
{
    const BATCH_QUEUE_NAME = 'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer';
    const LIVE_QUEUE_NAME = 'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer.Live';

    /**
     * @Flow\Inject
     * @var LoggerInterface
     */
    protected $logger;

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
     * @var NodeTypeMappingBuilderInterface
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
     * @Flow\InjectConfiguration(path="batchSize")
     * @var int
     */
    protected $batchSize;

    /**
     * Index all nodes by creating a new index and when everything was completed, switch the index alias.
     *
     * @param string $workspace
     * @throws ConfigurationException
     * @throws Exception
     * @throws StopCommandException
     * @throws \Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception
     * @throws \Flowpack\ElasticSearch\Exception
     * @throws \Neos\Flow\Http\Exception
     */
    public function buildCommand(string $workspace = null): void
    {
        $indexPostfix = (string) time();
        $indexName = $this->createNextIndex($indexPostfix);
        $this->updateMapping($indexPostfix);

        $this->outputLine();
        $this->outputLine('<b>Indexing on %s ...</b>', [$indexName]);

        $pendingJobs = $this->queueManager->getQueue(self::BATCH_QUEUE_NAME)->countReady();
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
     * @throws StopActionException
     * @throws StopCommandException
     */
    public function workCommand(string $queue = 'batch', int $exitAfter = null, int $limit = null, $verbose = false): void
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
    public function flushCommand(): void
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
            $queue = $this->queueManager->getQueue(self::BATCH_QUEUE_NAME);
            $this->outputLine('Pending Jobs   : %s', [$queue->countReady()]);
            $this->outputLine('Reserved Jobs  : %s', [$queue->countReserved()]);
            $this->outputLine('Failed Jobs    : %s', [$queue->countFailed()]);
        } catch (Exception $exception) {
            $this->outputLine('Pending Jobs   : Error, queue %s not found, %s', [self::BATCH_QUEUE_NAME, $exception->getMessage()]);
        }
    }

    /**
     * @param string $workspaceName
     * @param string $indexPostfix
     * @throws \Exception
     */
    protected function indexWorkspace(string $workspaceName, string $indexPostfix): void
    {
        $this->outputLine('<info>++</info> Indexing %s workspace', [$workspaceName]);
        $nodeCounter = 0;
        $offset = 0;
        while (true) {
            $iterator = $this->nodeDataRepository->findAllBySiteAndWorkspace($workspaceName, $offset, $this->batchSize);

            $jobData = [];

            foreach ($this->nodeDataRepository->iterate($iterator) as $data) {
                $jobData[] = [
                    'persistenceObjectIdentifier' => $data['persistenceObjectIdentifier'],
                    'identifier' => $data['identifier'],
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
            $offset += $this->batchSize;
            $this->persistenceManager->clearState();
        }
        $this->outputLine();
        $this->outputLine("\nNumber of Nodes to be indexed in workspace '%s': %d", [$workspaceName, $nodeCounter]);
        $this->outputLine();
    }

    /**
     * @param string $indexPostfix
     * @return string
     * @throws \Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception
     * @throws ConfigurationException
     * @throws \Flowpack\ElasticSearch\Exception
     * @throws \Neos\Flow\Http\Exception
     */
    protected function createNextIndex(string $indexPostfix): string
    {
        $this->nodeIndexer->setIndexNamePostfix($indexPostfix);
        $this->nodeIndexer->getIndex()->create();
        $this->logger->info(sprintf('Index %s created', $this->nodeIndexer->getIndexName()), LogEnvironment::fromMethodName(__METHOD__));

        return $this->nodeIndexer->getIndexName();
    }

    /**
     * Update Index Mapping
     *
     * @param string $indexPostfix
     * @return void
     * @throws ConfigurationException
     * @throws \Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception
     * @throws \Flowpack\ElasticSearch\Exception
     */
    protected function updateMapping(string $indexPostfix): void
    {
        $nodeTypeMappingCollection = $this->nodeTypeMappingBuilder->buildMappingInformation($this->nodeIndexer->getIndex());
        foreach ($nodeTypeMappingCollection as $mapping) {
            $this->nodeIndexer->setIndexNamePostfix($indexPostfix);
            /** @var Mapping $mapping */
            $mapping->apply();
        }
        $this->logger->info(sprintf('Mapping updated for index %s', $this->nodeIndexer->getIndexName()), LogEnvironment::fromMethodName(__METHOD__));
    }
}
