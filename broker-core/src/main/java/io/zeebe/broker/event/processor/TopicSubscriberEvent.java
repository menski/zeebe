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
package io.zeebe.broker.event.processor;

import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.*;
import org.agrona.DirectBuffer;

public class TopicSubscriberEvent extends UnpackedObject {
  // negative value for end of log
  protected LongProperty startPositionProp = new LongProperty("startPosition", -1L);

  protected IntegerProperty bufferSizeProp = new IntegerProperty("bufferSize");

  protected StringProperty nameProp = new StringProperty("name");

  // true if startPosition should override any previously acknowledged position
  protected BooleanProperty forceStartProp = new BooleanProperty("forceStart", false);

  public TopicSubscriberEvent() {
    this.declareProperty(startPositionProp)
        .declareProperty(nameProp)
        .declareProperty(bufferSizeProp)
        .declareProperty(forceStartProp);
  }

  public TopicSubscriberEvent setStartPosition(long startPosition) {
    this.startPositionProp.setValue(startPosition);
    return this;
  }

  public long getStartPosition() {
    return startPositionProp.getValue();
  }

  public TopicSubscriberEvent setBufferSize(int bufferSize) {
    this.bufferSizeProp.setValue(bufferSize);
    return this;
  }

  public int getBufferSize() {
    return bufferSizeProp.getValue();
  }

  public String getNameAsString() {
    final DirectBuffer stringBuffer = nameProp.getValue();
    return stringBuffer.getStringWithoutLengthUtf8(0, stringBuffer.capacity());
  }

  public DirectBuffer getName() {
    return nameProp.getValue();
  }

  public TopicSubscriberEvent setName(String name) {
    nameProp.setValue(name);
    return this;
  }

  public boolean getForceStart() {
    return forceStartProp.getValue();
  }
}
