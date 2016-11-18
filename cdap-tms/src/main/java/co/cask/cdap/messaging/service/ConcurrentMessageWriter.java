/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.messaging.service;

import com.google.common.base.Throwables;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Class to support writing to message/payload tables with high concurrency.
 *
 * It uses a non-blocking algorithm to batch writes from concurrent threads. The algorithm is the same as
 * the one used in ConcurrentStreamWriter.
 *
 * The algorithm is like this:
 *
 * When a thread that received a request, performs the following:
 *
 * <pre>
 * 1. Constructs a PendingWrite locally and enqueue it to a ConcurrentLinkedQueue.
 * 2. Use CAS to set an AtomicBoolean flag to true.
 * 3. If successfully set the flag to true, this thread becomes the writer and proceed to run step 4-7.
 * 4. Provides an Iterator of PendingWrite, which consumes from the ConcurrentLinkedQueue mentioned in step 1.
 * 5. The message table store method will consume the Iterator until it is empty
 * 6. Set the state of each PendingWrite that are written to COMPLETED (succeed/failure).
 * 7. Set the AtomicBoolean flag back to false.
 * 8. If the PendingWrite enqueued by this thread is NOT COMPLETED, go back to step 2.
 * </pre>
 *
 * The spin lock between step 2 to step 8 is necessary as it guarantees events enqueued by all threads would eventually
 * get written and flushed.
 */
@ThreadSafe
final class ConcurrentMessageWriter implements Closeable {

  private final AtomicBoolean writerFlag;
  private final PublishRequestWriter<?> messagesWriter;
  private final PendingWriteQueue pendingWriteQueue;
  private final AtomicBoolean closed;

  ConcurrentMessageWriter(PublishRequestWriter<?> messagesWriter) {
    this.messagesWriter = messagesWriter;
    this.writerFlag = new AtomicBoolean();
    this.pendingWriteQueue = new PendingWriteQueue();
    this.closed = new AtomicBoolean();
  }

  void persist(PublishRequest messages) throws IOException {
    if (closed.get()) {
      throw new IOException("Message writer is already closed");
    }

    PendingStoreRequest pendingWrite = new PendingStoreRequest(messages);
    pendingWriteQueue.enqueue(pendingWrite);

    while (!pendingWrite.isCompleted()) {
      if (!tryWrite()) {
        Thread.yield();
      }
    }
    if (!pendingWrite.isSuccess()) {
      Throwables.propagateIfInstanceOf(pendingWrite.getFailureCause(), IOException.class);
      throw new IOException("Unable to write message to " + messages.getTopicId(), pendingWrite.getFailureCause());
    }
  }

  private boolean tryWrite() {
    if (!writerFlag.compareAndSet(false, true)) {
      return false;
    }
    try {
      pendingWriteQueue.persist(messagesWriter);
    } finally {
      writerFlag.set(false);
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    // Flush everything in the queue.
    while (!tryWrite()) {
      Thread.yield();
    }
    messagesWriter.close();
  }

  /**
   * A resettable {@link Iterator} to provide {@link PublishRequest} to {@link PublishRequestWriter}.
   * Except the {@link #enqueue(PendingStoreRequest)} method, all methods on this class can only be
   * called while holding the writer flag.
   */
  private static final class PendingWriteQueue {

    private final Queue<PendingStoreRequest> writeQueue;
    private final List<PendingStoreRequest> inflightRequests;

    private PendingWriteQueue() {
      this.writeQueue = new ConcurrentLinkedQueue<>();
      this.inflightRequests = new ArrayList<>(100);
    }

    /**
     * Puts the given {@link PendingStoreRequest} to the concurrent queue.
     */
    void enqueue(PendingStoreRequest pendingWrite) {
      writeQueue.add(pendingWrite);
    }

    /**
     * Persists all {@link PendingStoreRequest} currently in the queue with the given writer.
     */
    void persist(PublishRequestWriter<?> writer) {
      // Capture all current events.
      // The reason for capturing instead of using a live iterator is to avoid the possible case of infinite write
      // time. E.g. while generating the entry to write to the storage table, a new store request get enqueued.
      inflightRequests.clear();
      PendingStoreRequest request = writeQueue.poll();
      while (request != null) {
        inflightRequests.add(request);
        request = writeQueue.poll();
      }
      try {
        writer.write(inflightRequests.iterator());
        completeAll(null);
      } catch (Throwable t) {
        completeAll(t);
      }
    }

    /**
     * Marks all inflight requests as collected through the {@link Iterator#next()} method as completed.
     * This method must be called while holding the writer flag.
     */
    void completeAll(@Nullable Throwable failureCause) {
      Iterator<PendingStoreRequest> itor = inflightRequests.iterator();
      while (itor.hasNext()) {
        itor.next().completed(failureCause);
        itor.remove();
      }
    }
  }
}
