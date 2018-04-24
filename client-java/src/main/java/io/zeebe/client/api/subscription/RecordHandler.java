package io.zeebe.client.api.subscription;

import io.zeebe.client.api.record.Record;

@FunctionalInterface
public interface RecordHandler
{

    void onRecord(Record record) throws Exception;
}
