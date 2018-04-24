package io.zeebe.client.api.record;

import java.util.List;

import io.zeebe.client.api.commands.DeploymentResource;

public interface DeploymentRecord extends Record
{
    /**
     * @return the name of the topic to deploy to
     */
    String getDeploymentTopic();

    /**
     * @return the resources to deploy
     */
    List<DeploymentResource> getResources();

}
