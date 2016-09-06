/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.kafka;

import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.kafka.KafkaPublisherExt.KafkaPublisherResult;
import org.apache.nifi.processors.kafka.test.EmbeddedKafka;
import org.apache.nifi.processors.kafka.test.EmbeddedKafkaProducerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

// The test is valid and should be ran when working on this module. @Ignore is
// to speed up the overall build
public class KafkaPublisherExtTest {

    private static EmbeddedKafka kafkaLocal;

    private static EmbeddedKafkaProducerHelper producerHelper;

    @BeforeClass
    public static void bforeClass() {
        kafkaLocal = new EmbeddedKafka();
        kafkaLocal.start();
        producerHelper = new EmbeddedKafkaProducerHelper(kafkaLocal);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        producerHelper.close();
        kafkaLocal.stop();
    }

    @Test
    public void validateSuccessfulSendAsWhole() throws Exception {
        InputStream contentStream = new ByteArrayInputStream("Hello Kafka".getBytes(StandardCharsets.UTF_8));
        String topicName = "validateSuccessfulSendAsWhole";

        Properties kafkaProperties = this.buildProducerProperties();
        KafkaPublisherExt publisher = new KafkaPublisherExt(kafkaProperties, mock(ComponentLog.class));

        PublishingContextExt publishingContext = new PublishingContextExt(contentStream, topicName);
        KafkaPublisherResult result = publisher.publish(publishingContext);

        assertEquals(0, result.getLastMessageAcked());
        assertEquals(1, result.getMessagesSent());
        contentStream.close();
        publisher.close();

        ConsumerIterator<byte[], byte[]> iter = this.buildConsumer(topicName);
        assertNotNull(iter.next());
        try {
            iter.next();
        } catch (ConsumerTimeoutException e) {
            // that's OK since this is the Kafka mechanism to unblock
        }
    }

    @Test
    public void validateSuccessfulSendAsDelimited() throws Exception {
        InputStream contentStream = new ByteArrayInputStream(
                "Hello Kafka\nHello Kafka\nHello Kafka\nHello Kafka\n".getBytes(StandardCharsets.UTF_8));
        String topicName = "validateSuccessfulSendAsDelimited";

        Properties kafkaProperties = this.buildProducerProperties();
        KafkaPublisherExt publisher = new KafkaPublisherExt(kafkaProperties, mock(ComponentLog.class));

        PublishingContextExt publishingContext = new PublishingContextExt(contentStream, topicName);
   //     publishingContext.setDelimiterBytes("\n".getBytes(StandardCharsets.UTF_8));
        KafkaPublisherResult result = publisher.publish(publishingContext);

        assertEquals(3, result.getLastMessageAcked());
        assertEquals(4, result.getMessagesSent());
        contentStream.close();
        publisher.close();

        ConsumerIterator<byte[], byte[]> iter = this.buildConsumer(topicName);
        assertNotNull(iter.next());
        assertNotNull(iter.next());
        assertNotNull(iter.next());
        assertNotNull(iter.next());
        try {
            iter.next();
            fail();
        } catch (ConsumerTimeoutException e) {
            // that's OK since this is the Kafka mechanism to unblock
        }
    }

    /*
     * This test simulates the condition where not all messages were ACKed by
     * Kafka
     */
    @Test
    public void validateRetries() throws Exception {
        byte[] testValue = "Hello Kafka1\nHello Kafka2\nHello Kafka3\nHello Kafka4\n".getBytes(StandardCharsets.UTF_8);
        InputStream contentStream = new ByteArrayInputStream(testValue);
        String topicName = "validateSuccessfulReSendOfFailedSegments";

        Properties kafkaProperties = this.buildProducerProperties();

        KafkaPublisherExt publisher = new KafkaPublisherExt(kafkaProperties, mock(ComponentLog.class));

        // simulates the first re-try
        int lastAckedMessageIndex = 1;
        PublishingContextExt publishingContext = new PublishingContextExt(contentStream, topicName, lastAckedMessageIndex);
     //   publishingContext.setDelimiterBytes("\n".getBytes(StandardCharsets.UTF_8));

        publisher.publish(publishingContext);

        ConsumerIterator<byte[], byte[]> iter = this.buildConsumer(topicName);
        String m1 = new String(iter.next().message());
        String m2 = new String(iter.next().message());
        assertEquals("Hello Kafka3\n", m1);
        assertEquals("Hello Kafka4\n", m2);
        try {
            iter.next();
            fail();
        } catch (ConsumerTimeoutException e) {
            // that's OK since this is the Kafka mechanism to unblock
        }

        // simulates the second re-try
        lastAckedMessageIndex = 2;
        contentStream = new ByteArrayInputStream(testValue);
        publishingContext = new PublishingContextExt(contentStream, topicName, lastAckedMessageIndex);
     //   publishingContext.setDelimiterBytes("\n".getBytes(StandardCharsets.UTF_8));
        publisher.publish(publishingContext);

        m1 = new String(iter.next().message());
        assertEquals("Hello Kafka4\n", m1);

        publisher.close();
    }

    /*
     * Similar to the above test, but it sets the first retry index to the last
     * possible message index and second index to an out of bound index. The
     * expectation is that no messages will be sent to Kafka
     */
    @Test
    public void validateRetriesWithWrongIndex() throws Exception {
        byte[] testValue = "Hello Kafka1\nHello Kafka2\nHello Kafka3\nHello Kafka4\n".getBytes(StandardCharsets.UTF_8);
        InputStream contentStream = new ByteArrayInputStream(testValue);
        String topicName = "validateRetriesWithWrongIndex";

        Properties kafkaProperties = this.buildProducerProperties();

        KafkaPublisherExt publisher = new KafkaPublisherExt(kafkaProperties, mock(ComponentLog.class));

        // simulates the first re-try
        int lastAckedMessageIndex = 3;
        PublishingContextExt publishingContext = new PublishingContextExt(contentStream, topicName, lastAckedMessageIndex);
     //   publishingContext.setDelimiterBytes("\n".getBytes(StandardCharsets.UTF_8));

        publisher.publish(publishingContext);

        ConsumerIterator<byte[], byte[]> iter = this.buildConsumer(topicName);

        try {
            iter.next();
            fail();
        } catch (ConsumerTimeoutException e) {
            // that's OK since this is the Kafka mechanism to unblock
        }

        // simulates the second re-try
        lastAckedMessageIndex = 6;
        contentStream = new ByteArrayInputStream(testValue);
        publishingContext = new PublishingContextExt(contentStream, topicName, lastAckedMessageIndex);
     //   publishingContext.setDelimiterBytes("\n".getBytes(StandardCharsets.UTF_8));
        publisher.publish(publishingContext);
        try {
            iter.next();
            fail();
        } catch (ConsumerTimeoutException e) {
            // that's OK since this is the Kafka mechanism to unblock
        }

        publisher.close();
    }

    @Test
    public void validateWithMultiByteCharactersNoDelimiter() throws Exception {
        String data = "僠THIS IS MY NEW TEXT.僠IT HAS A NEWLINE.";
        InputStream contentStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        String topicName = "validateWithMultiByteCharacters";

        Properties kafkaProperties = this.buildProducerProperties();

        KafkaPublisherExt publisher = new KafkaPublisherExt(kafkaProperties, mock(ComponentLog.class));
        PublishingContextExt publishingContext = new PublishingContextExt(contentStream, topicName);

        publisher.publish(publishingContext);
        publisher.close();

        ConsumerIterator<byte[], byte[]> iter = this.buildConsumer(topicName);
        String r = new String(iter.next().message(), StandardCharsets.UTF_8);
        assertEquals(data + "\n", r);
    }


    @Test
    public void validateRegexMatch() throws IOException {
        String data = "[USR-BACK] Be not afraid of greatness:\n" +
                "[USR-BACK] some are born great, some achieve greatness, and some have greatness thrust upon them.\n" +
                "The course of true love never did run smooth.\n" +
                "[USR-BACK] To thine own self be true, \n" +
                "and it must follow, \n" +
                "as the night the day, \n" +
                "thou canst not then be false to any man.\n";
        InputStream contentStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamTokenizer scanner = null;

        String topicName = "validateRegexMatch";





        Properties kafkaProperties = this.buildProducerProperties();
        KafkaPublisherExt publisher = new KafkaPublisherExt(kafkaProperties, mock(ComponentLog.class));

        PublishingContextExt publishingContext = new PublishingContextExt(contentStream, topicName);
        publishingContext.setCharset("UTF-8");
        publishingContext.setRegexDelimiter("(\\[\\S*\\])\\s+(.*)");
        KafkaPublisherResult result = publisher.publish(publishingContext);

        assertEquals(2, result.getLastMessageAcked());
        assertEquals(3, result.getMessagesSent());
        contentStream.close();
        publisher.close();

        ConsumerIterator<byte[], byte[]> iter = this.buildConsumer(topicName);
        assertEquals("[USR-BACK] Be not afraid of greatness:\n", new String(iter.next().message()));
        assertEquals("[USR-BACK] some are born great, some achieve greatness, and some have greatness thrust upon them.\nThe course of true love never did run smooth.\n", new String(iter.next().message()));
        assertEquals("[USR-BACK] To thine own self be true, \nand it must follow, \nas the night the day, \nthou canst not then be false to any man.\n", new String(iter.next().message()));
        try {
            iter.next();
            fail();
        } catch (ConsumerTimeoutException e) {
            // that's OK since this is the Kafka mechanism to unblock
        }


    }

    private Properties buildProducerProperties() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("key.serializer", ByteArraySerializer.class.getName());
        kafkaProperties.put("value.serializer", ByteArraySerializer.class.getName());
        kafkaProperties.setProperty("bootstrap.servers", "localhost:" + kafkaLocal.getKafkaPort());
        kafkaProperties.put("auto.create.topics.enable", "true");
        return kafkaProperties;
    }

    private ConsumerIterator<byte[], byte[]> buildConsumer(String topic) {
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:" + kafkaLocal.getZookeeperPort());
        props.put("group.id", "test");
        props.put("consumer.timeout.ms", "5000");
        props.put("auto.offset.reset", "smallest");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<>(1);
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        ConsumerIterator<byte[], byte[]> iter = streams.get(0).iterator();
        return iter;
    }
}
