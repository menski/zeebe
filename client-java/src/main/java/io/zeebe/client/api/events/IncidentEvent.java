package io.zeebe.client.api.events;

import io.zeebe.client.api.record.IncidentRecord;

public interface IncidentEvent extends IncidentRecord
{
    /**
     * @return the current state
     */
    IncidentState getState();

    enum IncidentState
    {
        CREATED,
        RESOLVED,
        RESOLVE_FAILED,
        DELETED,
    }
}
