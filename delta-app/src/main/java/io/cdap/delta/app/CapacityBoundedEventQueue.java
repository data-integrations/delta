/*
 *
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.delta.app;

import io.cdap.delta.api.ChangeEvent;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.Sequenced;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

  /**
   * A capacity bounded event queue. It will block if the capacity of the events in the queue reaches certain
   * limit. When calculating the capacity of an event only the size of the row and previous row in DML event are
   * calculated. Because other fields are fixed size and could be limited by queue size (the number of event in the
   * queue)
   */
  public class CapacityBoundedEventQueue extends ArrayBlockingQueue<Sequenced<? extends ChangeEvent>> {

    private final long capacity;
    private long usage;
    /** Main lock guarding all access */
    private final ReentrantLock lock;
    /** Condition for waiting puts */
    private final Condition notFull;

    public CapacityBoundedEventQueue(int queueSize, long capacity) {
      super(queueSize);
      this.capacity = capacity;
      this.usage = 0;
      this.lock = new ReentrantLock();
      this.notFull = lock.newCondition();
    }

    private static int computeSize(Sequenced<? extends ChangeEvent> event) {
      if (event == null) {
        return 0;
      }
      ChangeEvent changeEvent = event.getEvent();
      if (changeEvent instanceof DDLEvent) {
        return 0;
      }
      return ((DMLEvent) changeEvent).getSizeInBytes();
    }

    @Override
    public void put(Sequenced<? extends ChangeEvent> event) throws InterruptedException {
      lock.lockInterruptibly();
      try {
        int eventSize = computeSize(event);
        while (size() > 0 && usage + eventSize > capacity) {
          notFull.await();
        }
        super.put(event);
        usage += eventSize;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public boolean offer(Sequenced<? extends ChangeEvent> event) {
      lock.lock();
      try {
        int eventSize = computeSize(event);
        if (size() > 0 && usage + eventSize > capacity) {
          return false;
        } else {
          boolean result = super.offer(event);
          usage += eventSize;
          return result;
        }
      } finally {
        lock.unlock();
      }
    }

    @Override
    public boolean offer(Sequenced<? extends ChangeEvent> event, long timeout, TimeUnit unit)
      throws InterruptedException {
      long nanos = unit.toNanos(timeout);
      lock.lockInterruptibly();
      try {
        int eventSize = computeSize(event);
        while (size() > 0 && usage + eventSize > capacity) {
          if (nanos <= 0) {
            return false;
          }
          nanos = notFull.awaitNanos(nanos);
        }
        boolean result = super.offer(event, nanos, TimeUnit.NANOSECONDS);
        usage += eventSize;
        return result;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public Sequenced<? extends ChangeEvent> poll() {
      Sequenced<? extends ChangeEvent> event = super.poll();
      int eventSize = computeSize(event);

      if (eventSize > 0) {
        lock.lock();
        try {
          usage -= eventSize;
          notFull.signal();
        } finally {
          lock.unlock();
        }
      }
      return event;
    }

    @Override
    public Sequenced<? extends ChangeEvent> take() throws InterruptedException {
      Sequenced<? extends ChangeEvent> event = super.take();
      int eventSize = computeSize(event);
      if (eventSize > 0) {
        lock.lockInterruptibly();
        try {
          usage -= eventSize;
          notFull.signal();
        } finally {
          lock.unlock();
        }
      }
      return event;
    }

    @Override
    public Sequenced<? extends ChangeEvent> poll(long timeout, TimeUnit unit) throws InterruptedException {
      long nanos = unit.toNanos(timeout);
      Sequenced<? extends ChangeEvent> event = super.poll(timeout, unit);
      int eventSize = computeSize(event);
      if (eventSize > 0) {
        lock.lockInterruptibly();
        try {
          usage -= eventSize;
          notFull.signal();
        } finally {
          lock.unlock();
        }
      }
      return event;
    }
  }
