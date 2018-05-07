package io.zeebe.broker.system.deployment.service;

import io.zeebe.broker.system.deployment.data.LatestVersionByProcessIdAndTopicName;
import io.zeebe.broker.system.deployment.data.WorkflowKeyByProcessIdAndVersion;
import io.zeebe.broker.system.deployment.request.FetchWorkflowRequest;
import io.zeebe.broker.system.deployment.request.FetchWorkflowResponse;
import io.zeebe.clustering.management.FetchWorkflowRequestDecoder;
import io.zeebe.transport.*;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.ActorControl;
import org.agrona.DirectBuffer;

public class FetchWorkflowRequestHandler
{
    private final FetchWorkflowRequest fetchWorkflowRequest = new FetchWorkflowRequest();
    private final FetchWorkflowResponse fetchWorkflowResponse = new FetchWorkflowResponse();

    private final DeploymentWorkflowsCache deploymentWorkflowsCache;
    private final LatestVersionByProcessIdAndTopicName latestWorkflowKeyByProcessIdAndTopicName;
    private final WorkflowKeyByProcessIdAndVersion workflowKeyByProcessIdAndVersion;
    private final ActorControl actor;

    public FetchWorkflowRequestHandler(ActorControl actor,
        WorkflowKeyByProcessIdAndVersion workflowKeyByProcessIdAndVersion,
        LatestVersionByProcessIdAndTopicName latestWorkflowKeyByProcessIdAndTopicName,
        DeploymentWorkflowsCache deploymentWorkflowsCache)
    {
        this.actor = actor;
        this.workflowKeyByProcessIdAndVersion = workflowKeyByProcessIdAndVersion;
        this.latestWorkflowKeyByProcessIdAndTopicName = latestWorkflowKeyByProcessIdAndTopicName;
        this.deploymentWorkflowsCache = deploymentWorkflowsCache;
    }

    public void onFetchWorkfow(DirectBuffer buffer, int offset, int length, ServerOutput output, RemoteAddress remoteAddress, long requestId)
    {
        final DirectBuffer bufferCopy = BufferUtil.cloneBuffer(buffer, offset, length);

        actor.run(() ->
        {
            fetchWorkflowRequest.wrap(bufferCopy, 0, bufferCopy.capacity());

            long workflowKey = fetchWorkflowRequest.getWorkflowKey();

            if (workflowKey == FetchWorkflowRequestDecoder.workflowKeyNullValue())
            {
                int version = fetchWorkflowRequest.getVersion();
                final DirectBuffer bpmnProcessId = fetchWorkflowRequest.getBpmnProcessId();

                if (version == FetchWorkflowRequestDecoder.versionMaxValue())
                {
                    final DirectBuffer topicName = fetchWorkflowRequest.getTopicName();
                    version = latestWorkflowKeyByProcessIdAndTopicName.getLatestVersion(topicName, bpmnProcessId, -1);
                }

                workflowKey = workflowKeyByProcessIdAndVersion.get(bpmnProcessId, version, -1);
            }

            final DeploymentCachedWorkflow workflow = deploymentWorkflowsCache.getWorkflow(workflowKey);

            fetchWorkflowResponse.reset();

            if (workflow != null)
            {
                fetchWorkflowResponse.workflowKey(workflow.getWorkflowKey())
                    .version(workflow.getVersion())
                    .deploymentKey(workflow.getDeploymentKey())
                    .bpmnProcessId(workflow.getBpmnProcessId())
                    .bpmnXml(workflow.getBpmnXml());
            }

            final ServerResponse serverResponse = new ServerResponse()
                .writer(fetchWorkflowResponse)
                .requestId(requestId)
                .remoteAddress(remoteAddress);

            actor.runUntilDone(() ->
            {
                if (output.sendResponse(serverResponse))
                {
                    actor.done();
                }
                else
                {
                    actor.yield();
                }
            });
        });
    }
}
