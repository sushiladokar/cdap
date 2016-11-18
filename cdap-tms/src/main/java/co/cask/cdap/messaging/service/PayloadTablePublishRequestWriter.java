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
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.collect.AbstractIterator;

import java.io.IOException;
import java.util.Iterator;
import javax.annotation.concurrent.NotThreadSafe;

/**
 *
 */
@NotThreadSafe
final class PayloadTablePublishRequestWriter extends PublishRequestWriter<PayloadTable.Entry> {

  private final PayloadTable payloadTable;
  private final MutablePayloadTableEntry entry;

  PayloadTablePublishRequestWriter(PayloadTable payloadTable, TimeProvider timeProvider) {
    super(timeProvider);
    this.payloadTable = payloadTable;
    this.entry = new MutablePayloadTableEntry();
  }

  @Override
  protected Iterator<PayloadTable.Entry> transform(final PublishRequest messages,
                                                   final TimeSequenceProvider timeSequenceProvider) {
    entry.setTopicId(messages.getTopicId())
         .setTransactionWritePointer(messages.getTransactionWritePointer());

    return new AbstractIterator<PayloadTable.Entry>() {
      @Override
      protected PayloadTable.Entry computeNext() {
        if (!messages.hasNext()) {
          return endOfData();
        }
        // OK to cast the int to short, even it becomes -ve. The Payload only treat it as unsigned byte[]
        return entry.setPayload(messages.next())
          .setWriteTimestamp(timeSequenceProvider.getWriteTimestamp())
          .setSequenceId((short) timeSequenceProvider.getAndIncrementSequenceId());
      }
    };
  }

  @Override
  protected void doWrite(Iterator<PayloadTable.Entry> entries) throws IOException {
    payloadTable.store(entries);
  }

  @Override
  public void close() throws IOException {
    payloadTable.close();
  }

  /**
   * A mutable implementation of {@link PayloadTable.Entry}.
   */
  private static final class MutablePayloadTableEntry implements PayloadTable.Entry {

    private TopicId topicId;
    private long transactionWritePointer;
    private long writeTimestamp;
    private short sequenceId;
    private byte[] payload;

    MutablePayloadTableEntry setTopicId(TopicId topicId) {
      this.topicId = topicId;
      return this;
    }

    MutablePayloadTableEntry setTransactionWritePointer(long transactionWritePointer) {
      this.transactionWritePointer = transactionWritePointer;
      return this;
    }

    MutablePayloadTableEntry setWriteTimestamp(long writeTimestamp) {
      this.writeTimestamp = writeTimestamp;
      return this;
    }

    MutablePayloadTableEntry setSequenceId(short sequenceId) {
      this.sequenceId = sequenceId;
      return this;
    }

    MutablePayloadTableEntry setPayload(byte[] payload) {
      this.payload = payload;
      return this;
    }

    @Override
    public TopicId getTopicId() {
      return topicId;
    }

    @Override
    public byte[] getPayload() {
      return payload;
    }

    @Override
    public long getTransactionWritePointer() {
      return transactionWritePointer;
    }

    @Override
    public long getPayloadWriteTimestamp() {
      return writeTimestamp;
    }

    @Override
    public short getPayloadSequenceId() {
      return sequenceId;
    }
  }
}
