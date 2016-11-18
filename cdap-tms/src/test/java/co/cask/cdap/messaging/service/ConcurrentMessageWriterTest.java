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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.utils.TimeProvider;
import co.cask.cdap.messaging.data.Message;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Unit-test for {@link ConcurrentMessageWriter}.
 */
public class ConcurrentMessageWriterTest {

  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentMessageWriterTest.class);

  @Test
  public void testBasic() throws IOException {
    TopicId topicId1 = new NamespaceId("ns1").topic("t1");
    TopicId topicId2 = new NamespaceId("ns2").topic("t2");

    TestPublishRequestWriter testWriter = new TestPublishRequestWriter(new TimeProvider.IncrementalTimeProvider());
    ConcurrentMessageWriter writer = new ConcurrentMessageWriter(testWriter);
    writer.persist(new TestPublishRequest(topicId1, Arrays.asList("1", "2", "3")));

    // There should be 3 messages being written
    List<Message> messages = testWriter.getMessages().get(topicId1);
    Assert.assertEquals(3, messages.size());

    // All messages should be written with timestamp 0
    List<String> payloads = new ArrayList<>();
    for (Message message : messages) {
      Assert.assertEquals(0L, message.getId().getPublishTimestamp());
      payloads.add(Bytes.toString(message.getPayload()));
    }
    Assert.assertEquals(Arrays.asList("1", "2", "3"), payloads);

    // Write to another topic
    writer.persist(new TestPublishRequest(topicId2, Arrays.asList("a", "b", "c")));

    // There should be 3 messages being written to topic2
    messages = testWriter.getMessages().get(topicId2);
    Assert.assertEquals(3, messages.size());

    // All messages should be written with timestamp 1
    payloads.clear();
    for (Message message : messages) {
      Assert.assertEquals(1L, message.getId().getPublishTimestamp());
      payloads.add(Bytes.toString(message.getPayload()));
    }
    Assert.assertEquals(Arrays.asList("a", "b", "c"), payloads);
  }

  @Test
  public void testBasicConcurrentWrite() throws InterruptedException {
    final TopicId topicId = new NamespaceId("ns1").topic("t1");

    TestPublishRequestWriter testWriter = new TestPublishRequestWriter(new TimeProvider.IncrementalTimeProvider());
    final ConcurrentMessageWriter writer = new ConcurrentMessageWriter(testWriter);

    // Create two threads that writes. Both of them provide a slow iterator that will in between iteration
    // the slowing down is to make it write request from one thread is going to be written by another thread
    // who is the leader.
    ExecutorService executor = Executors.newFixedThreadPool(2);
    final CyclicBarrier barrier = new CyclicBarrier(2);
    for (int i = 0; i < 2; i++) {
      final int id = i;
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            barrier.await();
            List<String> messages = Arrays.asList(Integer.toString(id * 10),
                                                  Integer.toString(id * 10 + 1),
                                                  Integer.toString(id * 10 + 2));
            SlowIterator<String> payload = new SlowIterator<>(200, messages.iterator());
            LOG.info("Persisting {} with messages {}", id, messages);
            writer.persist(new TestPublishRequest(topicId, payload));
            LOG.info("Persisted {}", id);
          } catch (Exception e) {
            LOG.error("Exception", e);
          }
        }
      });
    }

    executor.shutdown();
    Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

    // Expected one of the thread will do all the writing
    Assert.assertEquals(6, testWriter.getLastWriteCount());

    // The ordering of the messages published from the same thread should be preserved.
    // Since there are two threads, and we can't control which thread actually enqueue first, there are two possible
    // ordering. Either [0, 1, 2, 10, 11, 12] or [10, 11, 12, 0, 1, 2]
    Iterable<String> messages = Iterables.transform(
      testWriter.getMessages().get(topicId), new Function<Message, String>() {
        @Override
        public String apply(Message message) {
          return Bytes.toString(message.getPayload());
        }
      });
    List<String> expected = Arrays.asList("0", "1", "2", "10", "11", "12");
    if (!Iterables.elementsEqual(expected, messages)) {
      expected = Arrays.asList("10", "11", "12", "0", "1", "2");
      Assert.assertTrue(Iterables.elementsEqual(expected, messages));
    }

    // Expect all messages to be published with the same timestamp, but different seq id.
    int seqId = 0;
    for (Message message : testWriter.getMessages().get(topicId)) {
      MessageId messageId = message.getId();
      Assert.assertEquals(0L, messageId.getPublishTimestamp());
      Assert.assertEquals(seqId++, messageId.getSequenceId());
    }
  }

  @Test
  public void testMaxSequence() throws IOException {
    // This test the case when a single PublishRequest has more than 65536 payload.
    // Expected entries beyond the max seqid will be rolled to the next timestamp with seqId reset to start from 0
    // Generate 65537 payloads
    int msgCount = PublishRequestWriter.SEQUENCE_ID_LIMIT + 1;
    List<String> payloads = new ArrayList<>(msgCount);
    for (int i = 0; i < msgCount; i++) {
      payloads.add(Integer.toString(i));
    }

    TopicId topicId = new NamespaceId("ns1").topic("t1");

    // Write the payloads
    TestPublishRequestWriter testWriter = new TestPublishRequestWriter(new TimeProvider.IncrementalTimeProvider());
    ConcurrentMessageWriter writer = new ConcurrentMessageWriter(testWriter);
    writer.persist(new TestPublishRequest(topicId, payloads));

    List<Message> messages = testWriter.getMessages().get(topicId);
    Assert.assertEquals(msgCount, messages.size());

    // The first 65536 messages should be with the same timestamp, with seqId from 0 to 65535
    for (int i = 0; i < msgCount - 1; i++) {
      MessageId id = messages.get(i).getId();
      Assert.assertEquals(0L, id.getPublishTimestamp());
      Assert.assertEquals((short) i, id.getSequenceId());
    }
    // The 65537th message should have a different timestamp and seqId = 0
    MessageId id = messages.get(msgCount - 1).getId();
    Assert.assertEquals(1L, id.getPublishTimestamp());
    Assert.assertEquals(0, id.getPayloadSequenceId());
  }

  /**
   * A {@link PublishRequestWriter} that turns all payloads to {@link Message} and stores it in a List.
   */
  private static final class TestPublishRequestWriter extends PublishRequestWriter<TestEntry> {

    private final ListMultimap<TopicId, Message> messages = ArrayListMultimap.create();
    private int lastWriteCount;

    TestPublishRequestWriter(TimeProvider timeProvider) {
      super(timeProvider);
    }

    @Override
    Iterator<TestEntry> transform(final PublishRequest messages, final TimeSequenceProvider timeSequenceProvider) {
      return Iterators.transform(messages, new Function<byte[], TestEntry>() {
        @Override
        public TestEntry apply(byte[] input) {
          return new TestEntry(messages.getTopicId(), messages.isTransactional(),
                               messages.getTransactionWritePointer(), timeSequenceProvider.getWriteTimestamp(),
                               (short) timeSequenceProvider.getAndIncrementSequenceId(), input);
        }
      });
    }

    @Override
    protected void doWrite(Iterator<TestEntry> entries) throws IOException {
      lastWriteCount = 0;
      while (entries.hasNext()) {
        TestEntry entry = entries.next();
        byte[] rawId = new byte[MessageId.RAW_ID_SIZE];
        MessageId.putRawId(entry.getWriteTimestamp(), entry.getSequenceId(), 0L, (short) 0, rawId, 0);
        byte[] payload = entry.getPayload();
        messages.put(entry.getTopicId(), new Message(new MessageId(rawId),
                                                     payload == null ? null : Arrays.copyOf(payload, payload.length)));
        lastWriteCount++;
      }
    }

    int getLastWriteCount() {
      return lastWriteCount;
    }

    void clear() {
      messages.clear();
    }

    ListMultimap<TopicId, Message> getMessages() {
      return messages;
    }

    @Override
    public void close() throws IOException {
      // No-op
    }
  }

  /**
   * An entry being by the {@link TestPublishRequestWriter}.
   */
  private static final class TestEntry {
    private final TopicId topicId;
    private final boolean transactional;
    private final long transactionWritePointer;
    private final long writeTimestamp;
    private final short sequenceId;
    private final byte[] payload;

    private TestEntry(TopicId topicId, boolean transactional, long transactionWritePointer,
                      long writeTimestamp, short sequenceId, @Nullable byte[] payload) {
      this.topicId = topicId;
      this.transactional = transactional;
      this.transactionWritePointer = transactionWritePointer;
      this.writeTimestamp = writeTimestamp;
      this.sequenceId = sequenceId;
      this.payload = payload;
    }

    public TopicId getTopicId() {
      return topicId;
    }

    public boolean isTransactional() {
      return transactional;
    }

    public long getTransactionWritePointer() {
      return transactionWritePointer;
    }

    public long getWriteTimestamp() {
      return writeTimestamp;
    }

    public short getSequenceId() {
      return sequenceId;
    }

    @Nullable
    public byte[] getPayload() {
      return payload;
    }
  }

  /**
   * A {@link PublishRequest} that takes a list of Strings as payload.
   */
  private static final class TestPublishRequest extends PublishRequest {

    private final Iterator<String> payloads;

    protected TestPublishRequest(TopicId topicId, List<String> payloads) {
      this(topicId, payloads.iterator());
    }

    protected TestPublishRequest(TopicId topicId, Iterator<String> payloads) {
      this(topicId, false, -1L, payloads);
    }

    protected TestPublishRequest(TopicId topicId, boolean transactional,
                                 long transactionWritePointer, Iterator<String> payloads) {
      super(topicId, transactional, transactionWritePointer);
      this.payloads = payloads;
    }

    @Nullable
    @Override
    protected byte[] doComputeNext() {
      return payloads.hasNext() ? Bytes.toBytes(payloads.next()) : null;
    }
  }

  /**
   * A {@link Iterator} that sleeps during iteration.
   *
   * @param <T> type of element
   */
  private static final class SlowIterator<T> extends AbstractIterator<T> {

    private final long sleepMillis;
    private final Iterator<T> iterator;

    private SlowIterator(long sleepMillis, Iterator<T> iterator) {
      this.sleepMillis = sleepMillis;
      this.iterator = iterator;
    }

    @Override
    protected T computeNext() {
      Uninterruptibles.sleepUninterruptibly(sleepMillis, TimeUnit.MILLISECONDS);
      return iterator.hasNext() ? iterator.next() : endOfData();
    }
  }
}
