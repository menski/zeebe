package io.zeebe.client.api.subscription;

import io.zeebe.client.api.commands.IncidentCommand;

@FunctionalInterface
public interface IncidentCommandHandler
{
    void onIncidentCommand(IncidentCommand incidentCommand);

    default void onIncidentCommandRejection(IncidentCommand incidentCommand)
    {
    };

}
