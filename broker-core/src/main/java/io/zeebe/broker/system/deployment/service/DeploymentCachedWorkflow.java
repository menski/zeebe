package io.zeebe.broker.system.deployment.service;

import io.zeebe.util.buffer.BufferUtil;
import org.agrona.DirectBuffer;

/**
 * Cached data about a workflow. Maintained by {@link DeploymentWorkflowsCache}.
 */
public class DeploymentCachedWorkflow
{
    private long workflowKey;
    private int version;
    private long deploymentKey;
    private DirectBuffer bpmnProcessId;
    private DirectBuffer bpmnXml;

    public long getWorkflowKey()
    {
        return workflowKey;
    }

    public DeploymentCachedWorkflow setWorkflowKey(long workflowKey)
    {
        this.workflowKey = workflowKey;
        return this;
    }

    public int getVersion()
    {
        return version;
    }

    public DeploymentCachedWorkflow setVersion(int version)
    {
        this.version = version;
        return this;
    }

    public long getDeploymentKey()
    {
        return deploymentKey;
    }

    public DeploymentCachedWorkflow setDeploymentKey(long deploymentKey)
    {
        this.deploymentKey = deploymentKey;
        return this;
    }

    public DirectBuffer getBpmnProcessId()
    {
        return bpmnProcessId;
    }

    public DirectBuffer getBpmnXml()
    {
        return bpmnXml;
    }

    public DeploymentCachedWorkflow putBpmnProcessId(DirectBuffer src)
    {
        this.bpmnProcessId = BufferUtil.cloneBuffer(src);
        return this;
    }

    public DeploymentCachedWorkflow putBpmnXml(DirectBuffer src)
    {
        this.bpmnXml = BufferUtil.cloneBuffer(src);
        return this;
    }
}
