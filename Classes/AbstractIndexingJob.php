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

use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Domain\Repository\NodeDataRepository;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Domain\Service\FakeNodeDataFactory;
use Flowpack\JobQueue\Common\Job\JobInterface;
use Neos\ContentRepository\Domain\Factory\NodeFactory;
use Neos\ContentRepository\Domain\Service\ContextFactoryInterface;
use Neos\ContentRepository\Search\Indexer\NodeIndexerInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Utility\Algorithms;
use Psr\Log\LoggerInterface;

abstract class AbstractIndexingJob implements JobInterface
{

    /**
     * @FLow\Inject
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @var NodeIndexerInterface
     * @Flow\Inject
     */
    protected $nodeIndexer;

    /**
     * @var NodeDataRepository
     * @Flow\Inject
     */
    protected $nodeDataRepository;

    /**
     * @var NodeFactory
     * @Flow\Inject
     */
    protected $nodeFactory;

    /**
     * @var ContextFactoryInterface
     * @Flow\Inject
     */
    protected $contextFactory;

    /**
     * @var FakeNodeDataFactory
     * @Flow\Inject
     */
    protected $fakeNodeDataFactory;

    /**
     * @var string
     */
    protected $identifier;

    /**
     * @var string
     */
    protected $targetWorkspaceName;

    /**
     * @var string
     */
    protected $indexPostfix;

    /**
     * @var array
     */
    protected $nodes = [];

    /**
     * @param string $indexPostfix
     * @param string $targetWorkspaceName In case indexing is triggered during publishing, a target workspace name will be passed in
     * @param array $nodes
     * @throws \Exception
     */
    public function __construct($indexPostfix, $targetWorkspaceName, array $nodes)
    {
        $this->identifier = Algorithms::generateRandomString(24);
        $this->targetWorkspaceName = $targetWorkspaceName;
        $this->indexPostfix = $indexPostfix;
        $this->nodes = $nodes;
    }

    /**
     * Get an optional identifier for the job
     *
     * @return string A job identifier
     */
    public function getIdentifier(): string
    {
        return $this->identifier;
    }
}
