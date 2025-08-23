/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.store.zeromq;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A ThreadFactory that names threads with a base name and a unique 4-digit number,
 * so threads will be named in the format: baseName-0001, baseName-0002, etc.
 * Thread names are important for the SimpleNodeStore: they are used as a prefix
 * for ZeroMQ topic subscriptions. If thread numbers are not 0-padded, then e.g.
 * someThread-1 will collide with someThread-10, someThread-11, etc.,
 * resulting in wrongly routed / lost messages.
 */
public class NamedThreadFactory implements ThreadFactory {
  private final String baseName;
  private final AtomicInteger count = new AtomicInteger(1);

  public NamedThreadFactory(String baseName) {
    this.baseName = baseName;
  }

  @Override
  public Thread newThread(@NotNull Runnable r) {
    String threadName = String.format("%s-%04d", baseName, count.getAndIncrement());
    return new Thread(r, threadName);
  }
}