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
package io.zeebe.client.impl.clustering;

import io.zeebe.client.api.commands.PartitionBrokerRole;
import io.zeebe.client.api.commands.PartitionInfo;

public class PartitionInfoImpl implements PartitionInfo {
  private String topicName;
  private int partitionId;
  private PartitionBrokerRole role;

  public PartitionInfoImpl setTopicName(final String topicName) {
    this.topicName = topicName;
    return this;
  }

  public PartitionInfoImpl setPartitionId(final int partitionId) {
    this.partitionId = partitionId;
    return this;
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public String getTopicName() {
    return topicName;
  }

  public PartitionInfoImpl setState(String state) {
    this.role = PartitionBrokerRole.valueOf(state);
    return this;
  }

  @Override
  public boolean isLeader() {
    return PartitionBrokerRole.LEADER.equals(role);
  }

  @Override
  public PartitionBrokerRole getRole() {
    return role;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("PartitionInfo [topicName=");
    builder.append(topicName);
    builder.append(", partitionId=");
    builder.append(partitionId);
    builder.append(", role=");
    builder.append(role);
    builder.append("]");
    return builder.toString();
  }
}
