package io.zeebe.client.api.events;

import java.util.List;

import io.zeebe.client.api.commands.Workflow;
import io.zeebe.client.api.record.DeploymentRecord;

public interface DeploymentEvent extends DeploymentRecord
{
    /**
     * @return the current state
     */
    DeploymentState getState();

    /**
     * @return the workflows which are deployed
     */
    List<Workflow> getDeployedWorkflows();

    enum DeploymentState
    {
        CREATED,

        VALIDATED,
        DISTRIBUTED,

        TIMED_OUT
    }

}
