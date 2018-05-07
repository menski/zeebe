package io.zeebe.broker.system.deployment.processor;

import io.zeebe.broker.clustering.orchestration.topic.TopicEvent;
import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.logstreams.processor.TypedEventProcessor;
import io.zeebe.broker.system.deployment.data.TopicNames;

public class DeploymentTopicCreatingEventProcessor implements TypedEventProcessor<TopicEvent>
{
    private final TopicNames topicNames;

    public DeploymentTopicCreatingEventProcessor(TopicNames topicNames)
    {
        this.topicNames = topicNames;
    }

    @Override
    public void updateState(TypedEvent<TopicEvent> event)
    {
        topicNames.addTopic(event.getValue().getName());
    }

}
