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
package io.zeebe.broker.logstreams.processor;

import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.intent.Intent;

/**
 * High-level record processor abstraction that implements the common behavior of most
 * command-handling processors.
 */
public interface CommandProcessor<T extends UnpackedObject>
{

    CommandResult onCommand(TypedRecord<T> command, CommandControl commandControl);

    default void updateStateOnAccept(TypedRecord<T> command)
    {
    };

    default void updateStateOnReject(TypedRecord<T> command)
    {
    };

    interface CommandControl
    {
        CommandResult accept(Intent newState);

        CommandResult reject(RejectionType type, String reason);
    }

    interface CommandResult
    {
    }
}
