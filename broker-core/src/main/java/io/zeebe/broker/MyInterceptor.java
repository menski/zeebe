/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker;

import io.zeebe.util.allocation.AllocatedDirectBuffer;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import net.bytebuddy.implementation.bind.annotation.This;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

public class MyInterceptor {

  public static void intercept(@This Object buffer) throws Exception {
    final ByteBuffer byteBuffer = ((UnsafeBuffer) buffer).byteBuffer();

    if (byteBuffer != null && byteBuffer.isDirect()) {
      final long address = BufferUtil.address(byteBuffer);
      final Throwable t;
      if ((t = AllocatedDirectBuffer.FREED_BUFFERS.get(address)) != null) {
        final StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        assert false
            : "Access to buffer at address " + address + " which was already freed by " + sw;
      }
    }
  }
}
