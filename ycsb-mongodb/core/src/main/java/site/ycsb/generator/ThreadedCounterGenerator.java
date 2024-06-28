/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.generator;

import site.ycsb.Utils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates a sequence of integers.
 * (0, 1, ...) * thread + start
 */
public class ThreadedCounterGenerator extends NumberGenerator {
  private final long start;
  private static final ThreadLocal<AtomicLong> tlCounter =
  new ThreadLocal<AtomicLong>() {
      @Override public AtomicLong initialValue() {
        return new AtomicLong();
      }
  };
  private final long threads;

  /**
   * Create a counter that starts at countstart.
   */
  public ThreadedCounterGenerator(long countstart, long threads) {
    start = countstart;
    this.threads = threads;
  }

  @Override
  public Long nextValue() {
    return threads * tlCounter.get().getAndIncrement() + Utils.threadId() + start;
  }

  @Override
  public Long lastValue() {
    return threads * tlCounter.get().get() + Utils.threadId() + start  -1;
  }

  @Override
  public double mean() {
    throw new UnsupportedOperationException("Can't compute mean of non-stationary distribution!");
  }
}
