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

import co.cask.cdap.common.utils.TimeProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.Uninterruptibles;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An abstract base class to abstract out writing of {@link Iterator} of {@link PublishRequest} to underlying
 * storage table.
 *
 * @param <T> Type of the internal entry that can be written by this writer.
 */
@NotThreadSafe
abstract class PublishRequestWriter<T> implements Closeable {

  // Sequence ID has 2 bytes, which the max value is 65535 (unsigned short).
  // Hence we cannot have more than 65536 messages with the same timestamp
  @VisibleForTesting
  static final int SEQUENCE_ID_LIMIT = 0x10000;

  private final LimitIterator<T> limitIterator;
  private final TimeSequenceProvider timeSequenceProvider;

  /**
   * Constructor.
   *
   * @param timeProvider the {@link TimeProvider} for generating timestamp to be used for write timestamp
   */
  PublishRequestWriter(TimeProvider timeProvider) {
    this.limitIterator = new LimitIterator<>();
    this.timeSequenceProvider = new TimeSequenceProvider(timeProvider);
  }

  /**
   * Writes the given list of {@link PendingStoreRequest} to this writer.
   */
  final void write(final Iterator<? extends PendingStoreRequest> requests) throws IOException {
    Iterator<T> entryIterator = new AbstractIterator<T>() {
      private PendingStoreRequest pendingStoreRequest;
      private Iterator<T> payloadIterator;

      @Override
      protected T computeNext() {
        while (payloadIterator == null || !payloadIterator.hasNext()) {
          if (pendingStoreRequest != null) {
            pendingStoreRequest.setEndTimestamp(timeSequenceProvider.getWriteTimestamp());
            pendingStoreRequest.setEndSequenceId(timeSequenceProvider.getSequenceId());
          }

          if (!requests.hasNext()) {
            return endOfData();
          }

          pendingStoreRequest = requests.next();
          pendingStoreRequest.setStartTimestamp(timeSequenceProvider.getWriteTimestamp());
          pendingStoreRequest.setStartSequenceId(timeSequenceProvider.getSequenceId());
          payloadIterator = transform(pendingStoreRequest, timeSequenceProvider);
        }
        return payloadIterator.hasNext() ? payloadIterator.next() : endOfData();
      }
    };

    timeSequenceProvider.update();
    int seqId = timeSequenceProvider.getSequenceId();
    while (entryIterator.hasNext()) {
      doWrite(limitIterator.reset(entryIterator, SEQUENCE_ID_LIMIT - seqId));
      timeSequenceProvider.update();
      seqId = timeSequenceProvider.getSequenceId();
    }
  }

  abstract Iterator<T> transform(PublishRequest messages, TimeSequenceProvider timeSequenceProvider);

  abstract void doWrite(Iterator<T> entries) throws IOException;

  /**
   * Provider for providing write timestamp and write sequence id.
   */
  static final class TimeSequenceProvider {

    private final TimeProvider timeProvider;
    private long writeTimestamp;
    private long lastWriteTimestamp;
    private int seqId;

    TimeSequenceProvider(TimeProvider timeProvider) {
      this.timeProvider = timeProvider;
      this.lastWriteTimestamp = -1L;
    }

    long getWriteTimestamp() {
      return writeTimestamp;
    }

    int getAndIncrementSequenceId() {
      return seqId++;
    }

    private void update() {
      writeTimestamp = timeProvider.currentTimeMillis();
      if (writeTimestamp == lastWriteTimestamp && seqId >= SEQUENCE_ID_LIMIT) {
        // Force the writeTimestamp to advance if we used up all sequence id.
        Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.MILLISECONDS);
        writeTimestamp = timeProvider.currentTimeMillis();
      }

      if (writeTimestamp != lastWriteTimestamp) {
        lastWriteTimestamp = writeTimestamp;
        seqId = 0;
      }
    }

    private int getSequenceId() {
      return seqId;
    }
  }

  /**
   * A resettable {@link Iterator} that will iterate up to the given limit.
   * @param <T> type of element
   */
  private static final class LimitIterator<T> implements Iterator<T> {

    private Iterator<T> delegate;
    private int limit;

    LimitIterator<T> reset(Iterator<T> delegate, int limit) {
      this.delegate = delegate;
      this.limit = limit;
      return this;
    }

    @Override
    public boolean hasNext() {
      return limit > 0 && delegate.hasNext();
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      limit--;
      return delegate.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
    }
  }
}
