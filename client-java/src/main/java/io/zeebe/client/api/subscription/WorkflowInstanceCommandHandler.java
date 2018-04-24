package io.zeebe.client.api.subscription;

import io.zeebe.client.api.commands.WorkflowInstanceCommand;

@FunctionalInterface
public interface WorkflowInstanceCommandHandler
{

    void onWorkflowInstanceCommand(WorkflowInstanceCommand workflowInstanceCommand);

    default void onWorkflowInstanceCommandRejection(WorkflowInstanceCommand workflowInstanceCommand)
    {
    };
}
