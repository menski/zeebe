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
package io.zeebe.util.allocation;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import org.agrona.BufferUtil;

public class AllocatedDirectBuffer extends AllocatedBuffer {
  private final Consumer<AllocatedDirectBuffer> onCloseCallback;

  public static final Set<Long> FREED_BUFFERS = new HashSet<>();

  public AllocatedDirectBuffer(ByteBuffer buffer, Consumer<AllocatedDirectBuffer> onClose) {
    super(buffer);
    this.onCloseCallback = onClose;
  }

  @Override
  public void doClose() {
    LOG.info("Freeing buffer at address {}", BufferUtil.address(rawBuffer));
    FREED_BUFFERS.add(BufferUtil.address(rawBuffer));
    AllocationUtil.freeDirectBuffer(rawBuffer);

    onCloseCallback.accept(this);
  }
}
