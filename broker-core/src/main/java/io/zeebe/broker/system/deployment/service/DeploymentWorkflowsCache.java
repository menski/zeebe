package io.zeebe.broker.system.deployment.service;

import java.util.Iterator;

import io.zeebe.broker.system.deployment.data.DeploymentPositionByWorkflowKey;
import io.zeebe.broker.workflow.data.DeployedWorkflow;
import io.zeebe.broker.workflow.data.DeploymentEvent;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LoggedEvent;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Caches workflow information for answering requests
 */
public class DeploymentWorkflowsCache
{
    private Long2ObjectHashMap<DeploymentCachedWorkflow> cachedWorkflowsByKey = new Long2ObjectHashMap<>();

    private final DeploymentPositionByWorkflowKey deploymentPositionByWorkflowKey;

    private final BufferedLogStreamReader reader;

    private final DeploymentEvent deploymentEvent = new DeploymentEvent();

    public DeploymentWorkflowsCache(final BufferedLogStreamReader reader,
        DeploymentPositionByWorkflowKey deploymentPositionByWorkflowKey)
    {
        this.reader = reader;
        this.deploymentPositionByWorkflowKey = deploymentPositionByWorkflowKey;
    }

    public DeploymentCachedWorkflow getWorkflow(long key)
    {
        DeploymentCachedWorkflow cachedWorkflow = cachedWorkflowsByKey.get(key);

        if (cachedWorkflow == null)
        {
            cachedWorkflow = resolveWorkflow(key);
        }

        return cachedWorkflow;
    }

    private DeploymentCachedWorkflow resolveWorkflow(long key)
    {
        final long position = deploymentPositionByWorkflowKey.get(key, -1);

        if (position != -1)
        {
            if (reader.seek(position))
            {
                final LoggedEvent event = reader.next();

                event.readValue(deploymentEvent);

                final Iterator<DeployedWorkflow> deployedWorkflowsIterator = deploymentEvent.deployedWorkflows().iterator();

                while (deployedWorkflowsIterator.hasNext())
                {
                    final DeployedWorkflow deployedWorkflow = deployedWorkflowsIterator.next();

                    if (deployedWorkflow.getKey() == key)
                    {
                        return new DeploymentCachedWorkflow()
                            .setVersion(deployedWorkflow.getVersion())
                            .setWorkflowKey(key)
                            .setDeploymentKey(event.getKey())
                            .putBpmnProcessId(deployedWorkflow.getBpmnProcessId())
                            .putBpmnXml(deploymentEvent.resources().iterator().next().getResource());
                    }
                }
            }
        }

        return null;
    }
}
