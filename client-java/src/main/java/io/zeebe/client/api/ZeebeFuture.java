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
package io.zeebe.client.api;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface ZeebeFuture<T> extends Future<T> {

  /** Like {@link #get()} but throws runtime exceptions. */
  T join();

  /** Like {@link #get(long, java.util.concurrent.TimeUnit)} but throws runtime exceptions. */
  T join(long timeout, TimeUnit unit);
}
