<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Domain\Service;

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\ContentRepository\Domain\Repository\WorkspaceRepository;
use Neos\ContentRepository\Domain\Service\NodeTypeManager;
use Neos\Flow\Annotations as Flow;

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

    public function createFromPayload(array $data)
    {
        if (!isset($data['workspace']) || empty($data['workspace'])) {
            throw new Exception('Unable to create fake node data, missing workspace value', 1508448007);
        }
        if (!isset($data['path']) || empty($data['path'])) {
            throw new Exception('Unable to create fake node data, missing path value', 1508448008);
        }
        if (!isset($data['nodeIdentifier']) || empty($data['nodeIdentifier'])) {
            throw new Exception('Unable to create fake node data, missing identifier value', 1508448009);
        }
        if (!isset($data['nodeType']) || empty($data['nodeType'])) {
            throw new Exception('Unable to create fake node data, missing nodeType value', 1508448011);
        }

        $workspace = $this->workspaceRepository->findOneByName($data['workspace']);
        if ($workspace === null) {
            throw new Exception('Unable to create fake node data, workspace not found', 1508448028);
        }

        $nodeData = new NodeData($data['path'], $workspace, $data['nodeIdentifier'], isset($data['dimensions']) ? $data['dimensions'] : null);
        $nodeData->setNodeType($this->nodeTypeManager->getNodeType($data['nodeType']));

        $nodeData->setProperty('title', 'Fake node');
        $nodeData->setProperty('uriPathSegment', 'fake-node');

        $nodeData->setRemoved(true);

        return $nodeData;
    }
}
