package io.zeebe.client.api.commands;

import io.zeebe.client.api.record.DeploymentRecord;

public interface DeploymentCommand extends DeploymentRecord
{
    /**
     * @return the command name
     */
    DeploymentCommandName getName();

    enum DeploymentCommandName
    {
        CREATE
    }

}
