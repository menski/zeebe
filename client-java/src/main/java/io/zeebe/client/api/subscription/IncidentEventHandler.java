package io.zeebe.client.api.subscription;

import io.zeebe.client.api.events.IncidentEvent;

@FunctionalInterface
public interface IncidentEventHandler
{

    void onIncidentEvent(IncidentEvent incidentEvent);
}
