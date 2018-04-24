package io.zeebe.client.api.commands;

import io.zeebe.client.api.record.JobRecord;

public interface JobCommand extends JobRecord
{

    JobCommandName getName();

    enum JobCommandName
    {
        CREATE,
        LOCK,
        COMPLETE,
        EXPIRE_LOCK,
        FAIL,
        UPDATE_RETRIES,
        CANCEL,
    }
}
