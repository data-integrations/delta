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

    private final long capacityLimit;
    private long capacity;
    /** Main lock guarding all access */
    private final ReentrantLock lock;
    /** Condition for waiting puts */
    private final Condition notFull;

    public CapacityBoundedEventQueue(int queueSize, long capcityLimit) {
      super(queueSize);
      this.capacityLimit = capcityLimit;
      this.capacity = 0;
      this.lock = new ReentrantLock();
      this.notFull = lock.newCondition();
    }

    private static void checkNotNull(Sequenced<? extends ChangeEvent> event) {
      if (event == null) {
        throw new NullPointerException();
      }
    }

    private static int computeCapacity(Sequenced<? extends ChangeEvent> event) {
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
      checkNotNull(event);
      final ReentrantLock lock = this.lock;
      lock.lockInterruptibly();
      try {
        int eventCapacity = computeCapacity(event);
        while (capacity + eventCapacity > capacityLimit) {
          notFull.await();
        }
        super.put(event);
        capacity += eventCapacity;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public boolean offer(Sequenced<? extends ChangeEvent> event) {
      checkNotNull(event);
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
        int eventCapacity = computeCapacity(event);
        if (capacity + eventCapacity > capacityLimit) {
          return false;
        } else {
          boolean result = super.offer(event);
          capacity += eventCapacity;
          return result;
        }
      } finally {
        lock.unlock();
      }
    }

    @Override
    public boolean offer(Sequenced<? extends ChangeEvent> event, long timeout, TimeUnit unit)
      throws InterruptedException {
      checkNotNull(event);
      long nanos = unit.toNanos(timeout);
      final ReentrantLock lock = this.lock;
      lock.lockInterruptibly();
      try {
        int eventCapacity = computeCapacity(event);
        while (capacity + eventCapacity > capacityLimit) {
          if (nanos <= 0) {
            return false;
          }
          nanos = notFull.awaitNanos(nanos);
        }
        boolean result = super.offer(event, nanos, TimeUnit.NANOSECONDS);
        capacity += eventCapacity;
        return result;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public Sequenced<? extends ChangeEvent> poll() {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
        Sequenced<? extends ChangeEvent> event = super.poll();
        int eventCapacity = computeCapacity(event);
        if (eventCapacity > 0) {
          capacity -= eventCapacity;
          notFull.signal();
        }
        return event;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public Sequenced<? extends ChangeEvent> take() throws InterruptedException {
      final ReentrantLock lock = this.lock;
      lock.lockInterruptibly();
      try {
        Sequenced<? extends ChangeEvent> event = super.take();
        int eventCapacity = computeCapacity(event);
        if (eventCapacity > 0) {
          capacity -= eventCapacity;
          notFull.signal();
        }
        return event;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public Sequenced<? extends ChangeEvent> poll(long timeout, TimeUnit unit) throws InterruptedException {
      long nanos = unit.toNanos(timeout);
      final ReentrantLock lock = this.lock;
      lock.lockInterruptibly();
      try {
        Sequenced<? extends ChangeEvent> event = super.poll(timeout, unit);
        int eventCapacity = computeCapacity(event);
        if (eventCapacity > 0) {
          capacity -= eventCapacity;
          notFull.signal();
        }
        return event;
      } finally {
        lock.unlock();
      }
    }
  }
