<?php
declare(strict_types=1);

namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Domain\Service;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentRepositoryQueueIndexer package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\ContentRepository\Domain\Repository\WorkspaceRepository;
use Neos\ContentRepository\Domain\Service\NodeTypeManager;
use Neos\ContentRepository\Exception\NodeTypeNotFoundException;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Persistence\Doctrine\PersistenceManager;

/**
 * @Flow\Scope("singleton")
 */
class FakeNodeDataFactory
{
    /**
     * @var WorkspaceRepository
     * @Flow\Inject
     */
    protected $workspaceRepository;

    /**
     * @var NodeTypeManager
     * @Flow\Inject
     */
    protected $nodeTypeManager;

    /**
     * @var PersistenceManager
     * @Flow\Inject
     */
    protected $persistenceManager;

    /**
     * This creates a "fake" removed NodeData instance from the given payload
     *
     * @param array $payload
     * @return NodeData
     * @throws Exception
     */
    public function createFromPayload(array $payload): NodeData
    {
        if (!isset($payload['workspace']) || empty($payload['workspace'])) {
            throw new Exception('Unable to create fake node data, missing workspace value', 1508448007);
        }
        if (!isset($payload['path']) || empty($payload['path'])) {
            throw new Exception('Unable to create fake node data, missing path value', 1508448008);
        }
        if (!isset($payload['identifier']) || empty($payload['identifier'])) {
            throw new Exception('Unable to create fake node data, missing identifier value', 1508448009);
        }
        if (!isset($payload['nodeType']) || empty($payload['nodeType'])) {
            throw new Exception('Unable to create fake node data, missing nodeType value', 1508448011);
        }

        $workspace = $this->workspaceRepository->findOneByName($payload['workspace']);
        if ($workspace === null) {
            throw new Exception('Unable to create fake node data, workspace not found', 1508448028);
        }

        $nodeData = new NodeData($payload['path'], $workspace, $payload['identifier'], isset($payload['dimensions']) ? $payload['dimensions'] : null);
        try {
            $nodeData->setNodeType($this->nodeTypeManager->getNodeType($payload['nodeType']));
        } catch (NodeTypeNotFoundException $e) {
            throw new Exception('Unable to create fake node data, node type not found', 1509362172);
        }

        $nodeData->setProperty('title', 'Fake node');
        $nodeData->setProperty('uriPathSegment', 'fake-node');

        $nodeData->setRemoved(true);

        // Ensure, the fake-node is not persisted
        if ($this->persistenceManager->isNewObject($nodeData) === false) {
            $this->persistenceManager->remove($nodeData);
        }

        return $nodeData;
    }
}
