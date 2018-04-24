package io.zeebe.client.api.subscription;

import io.zeebe.client.api.events.WorkflowInstanceEvent;

@FunctionalInterface
public interface WorkflowInstanceEventHandler
{
    void onWorkflowInstanceEvent(WorkflowInstanceEvent workflowInstanceEvent);
}
