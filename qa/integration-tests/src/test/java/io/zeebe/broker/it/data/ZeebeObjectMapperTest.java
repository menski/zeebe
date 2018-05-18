/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.broker.it.data;

import static io.zeebe.broker.it.util.TopicEventRecorder.state;
import static io.zeebe.test.util.TestUtil.waitUntil;

import io.zeebe.broker.it.ClientRule;
import io.zeebe.broker.it.EmbeddedBrokerRule;
import io.zeebe.broker.it.util.RecordingJobHandler;
import io.zeebe.broker.it.util.TopicEventRecorder;
import io.zeebe.client.api.events.JobEvent;
import io.zeebe.client.api.events.JobEvent.JobState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class ZeebeObjectMapperTest
{
    public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();

    public ClientRule clientRule = new ClientRule();

    public TopicEventRecorder eventRecorder = new TopicEventRecorder(clientRule, false);

    @Rule
    public RuleChain ruleChain = RuleChain
        .outerRule(brokerRule)
        .around(clientRule)
        .around(eventRecorder);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldCompleteJob() throws InterruptedException
    {
        // given
        eventRecorder.startRecordingEvents();

        clientRule.getJobClient()
                .newCreateCommand()
                .jobType("foo")
                .payload("{\"foo\":\"bar\"}")
                .send();

        final RecordingJobHandler jobHandler = new RecordingJobHandler();

        clientRule.getSubscriptionClient()
            .newJobSubscription()
            .jobType("foo")
            .handler(jobHandler)
            .open();

        waitUntil(() -> !jobHandler.getHandledJobs().isEmpty());
        final JobEvent job = jobHandler.getHandledJobs().get(0);

        final String json = job.toJson();

        System.out.println(">>>>> " + json);

        final JobEvent restoredJobEvent = clientRule.getClient().objectMapper().fromJson(json, JobEvent.class);

        // when
        clientRule.getJobClient()
            .newCompleteCommand(restoredJobEvent)
            .payload("{\"foo\":\"baz\"}")
            .send()
            .join();

        // then
        waitUntil(() -> eventRecorder.hasJobEvent(state(JobState.COMPLETED)));

        System.out.println(">> " + eventRecorder.getJobEvents(JobState.COMPLETED).get(0));
    }

}
