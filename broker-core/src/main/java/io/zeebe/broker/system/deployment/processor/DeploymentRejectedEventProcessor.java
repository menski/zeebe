package io.zeebe.broker.system.deployment.processor;

import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.broker.workflow.data.DeploymentEvent;

public class DeploymentRejectedEventProcessor implements TypedEventProcessor<DeploymentEvent>
{
    @Override
    public boolean executeSideEffects(TypedEvent<DeploymentEvent> event, TypedResponseWriter responseWriter)
    {
        return responseWriter.write(event);
    }
}
