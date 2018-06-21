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

import io.zeebe.util.CloseableSilently;
import io.zeebe.util.Loggers;
import java.nio.ByteBuffer;
import org.slf4j.Logger;

public abstract class AllocatedBuffer implements CloseableSilently {

  protected static final Logger LOG = Loggers.ALLOCATION_LOGGER;

  protected final ByteBuffer rawBuffer;
  private volatile boolean closed;

  public AllocatedBuffer(ByteBuffer buffer) {
    this.rawBuffer = buffer;
    this.closed = false;
  }

  public ByteBuffer getRawBuffer() {
    return rawBuffer;
  }

  public int capacity() {
    return rawBuffer.capacity();
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      doClose();
    }
  }

  public abstract void doClose();

  @Override
  protected void finalize() throws Throwable {
    if (!isClosed()) {
      LOG.warn(
          "Allocated {} bytes{}, which are not released. Releasing bytes.",
          getRawBuffer().capacity(),
          rawBuffer.isDirect() ? " direct" : "");
      close();
    }
  }
}
