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
package io.zeebe.example.topic;

import io.zeebe.client.ZeebeClient;
import io.zeebe.client.ZeebeClientBuilder;
import io.zeebe.client.api.events.TopicEvent;
import java.time.Duration;
import java.util.UUID;

public class TopicCreator {

  public static void main(final String[] args) throws InterruptedException {

      final int iterations = 10;
      final Duration delay = Duration.ofSeconds(1);

    final String broker = "localhost:51015";
    final int partitions = 10;
    final int replicationFactor = 3;

    final ZeebeClientBuilder clientBuilder =
        ZeebeClient.newClientBuilder().brokerContactPoint(broker);

    try (ZeebeClient client = clientBuilder.build()) {
      for (int i = 0; i < iterations; i++) {
        final String topic = UUID.randomUUID().toString();
        System.out.println(
            "Creating topic "
                + topic
                + " with "
                + partitions
                + " partition(s) "
                + "with contact point "
                + broker);

        final TopicEvent topicEvent =
            client
                .newCreateTopicCommand()
                .name(topic)
                .partitions(partitions)
                .replicationFactor(replicationFactor)
                .send()
                .join();

          System.out.println(topicEvent.getState());
          Thread.sleep(delay.toMillis());
      }
    }
  }
}
