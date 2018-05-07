package io.zeebe.broker.system.deployment.data;

import io.zeebe.msgpack.value.LongValue;

/**
 * Used for assigning workflow keys
 */
public class LastWorkflowKey
{
    private LongValue lastWorkflowKey = new LongValue();

    public long incrementAndGet()
    {
        final long value = lastWorkflowKey.getValue() + 1;

        lastWorkflowKey.setValue(value);

        return value;
    }

    public LongValue getRawValue()
    {
        return lastWorkflowKey;
    }
}
