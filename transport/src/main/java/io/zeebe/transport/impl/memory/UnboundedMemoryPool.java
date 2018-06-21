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
package io.zeebe.transport.impl.memory;

import io.zeebe.transport.Loggers;
import java.nio.ByteBuffer;
import org.slf4j.Logger;

/**
 * used for transports where you do not need to limit memory (like client requests in the zeebe
 * broker)
 */
public class UnboundedMemoryPool implements TransportMemoryPool {
  private static final Logger LOG = Loggers.TRANSPORT_MEMORY_LOGGER;

  @Override
  public ByteBuffer allocate(int requestedCapacity) {
    LOG.trace("Attocated {} bytes", requestedCapacity);
      assert requestedCapacity > 0 : requestedCapacity;
    return ByteBuffer.allocate(requestedCapacity);
  }

  @Override
  public void reclaim(ByteBuffer buffer) {
    final int bytesReclaimed = buffer.capacity();
    LOG.trace("Reclaiming {} bytes", bytesReclaimed);
  }
}
