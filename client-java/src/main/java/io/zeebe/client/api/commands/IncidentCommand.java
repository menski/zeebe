package io.zeebe.client.api.commands;

import io.zeebe.client.api.record.IncidentRecord;

public interface IncidentCommand extends IncidentRecord
{
    IncidentCommandName getName();

    enum IncidentCommandName
    {
        CREATE,
        RESOLVE,
        DELETE,
    }
}
