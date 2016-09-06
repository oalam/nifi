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

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.nifi.processors.kafka.test.EmbeddedKafka;
import org.apache.nifi.processors.kafka.test.EmbeddedKafkaProducerHelper;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

// The test is valid and should be ran when working on this module. @Ignore is
// to speed up the overall build
public class PutKafkaExtTest {

    private static EmbeddedKafka kafkaLocal;

    private static EmbeddedKafkaProducerHelper producerHelper;

    @BeforeClass
    public static void beforeClass() {
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
    public void validateSingleCharacterDemarcatedMessages() {
        String topicName = "validateSingleCharacterDemarcatedMessages";
        PutKafkaExt putKafka = new PutKafkaExt();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafkaExt.TOPIC, topicName);
        runner.setProperty(PutKafkaExt.CLIENT_NAME, "foo");
        runner.setProperty(PutKafkaExt.KEY, "key1");
        runner.setProperty(PutKafkaExt.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafkaExt.ZOOKEEPER_CONNECTION_STRING, "localhost:" + kafkaLocal.getZookeeperPort());
        runner.setProperty(PutKafkaExt.MESSAGE_REGEX_DELIMITER, ".*");


        runner.enqueue("Hello World\nGoodbye\n1\n2\n3\n4\n5".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PutKafkaExt.REL_SUCCESS, 1);
        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);


        assertEquals("Hello World\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("Goodbye\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("1\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("2\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("3\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("4\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("5\n", new String(consumer.next().message(), StandardCharsets.UTF_8));

        runner.shutdown();
    }

    @Test
    public void validateMultiCharacterDelimitedMessages() {
        String topicName = "validateMultiCharacterDemarcatedMessagesAndCustomPartitioner";
        PutKafkaExt putKafka = new PutKafkaExt();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafkaExt.TOPIC, topicName);
        runner.setProperty(PutKafkaExt.CLIENT_NAME, "foo");
        runner.setProperty(PutKafkaExt.KEY, "key1");
        runner.setProperty(PutKafkaExt.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafkaExt.ZOOKEEPER_CONNECTION_STRING, "localhost:" + kafkaLocal.getZookeeperPort());
        runner.setProperty(PutKafkaExt.MESSAGE_REGEX_DELIMITER, "foo(.*)");

        runner.enqueue("fooHello World\nfooGoodbye\nfoo1\nfoo2\nfoo3\nfoo4\nfoo5".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PutKafkaExt.REL_SUCCESS, 1);
        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);
        assertEquals("fooHello World\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("fooGoodbye\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("foo1\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("foo2\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("foo3\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("foo4\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("foo5\n", new String(consumer.next().message(), StandardCharsets.UTF_8));

        runner.shutdown();
    }

    @Test
    public void validateDemarcationIntoEmptyMessages() {
        String topicName = "validateDemarcationIntoEmptyMessages";
        PutKafkaExt putKafka = new PutKafkaExt();
        final TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafkaExt.TOPIC, topicName);
        runner.setProperty(PutKafkaExt.KEY, "key1");
        runner.setProperty(PutKafkaExt.CLIENT_NAME, "foo");
        runner.setProperty(PutKafkaExt.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafkaExt.ZOOKEEPER_CONNECTION_STRING, "localhost:" + kafkaLocal.getZookeeperPort());
        runner.setProperty(PutKafkaExt.MESSAGE_REGEX_DELIMITER, ".*");

        final byte[] bytes = "\n\n\n1\n2\n".getBytes(StandardCharsets.UTF_8);
        runner.enqueue(bytes);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKafkaExt.REL_SUCCESS, 1);

        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);

        assertNotNull(consumer.next());
        assertNotNull(consumer.next());
        assertNotNull(consumer.next());
        assertNotNull(consumer.next());
        assertNotNull(consumer.next());
        try {
            consumer.next();
            fail();
        } catch (Exception e) {
            // ignore
        }
    }

    @Test
    public void validateComplexRightPartialDemarcatedMessages() {
        String topicName = "validateComplexRightPartialDemarcatedMessages";
        PutKafkaExt putKafka = new PutKafkaExt();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafkaExt.TOPIC, topicName);
        runner.setProperty(PutKafkaExt.CLIENT_NAME, "foo");
        runner.setProperty(PutKafkaExt.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafkaExt.ZOOKEEPER_CONNECTION_STRING, "localhost:" + kafkaLocal.getZookeeperPort());
        runner.setProperty(PutKafkaExt.MESSAGE_REGEX_DELIMITER, "僠<僠WILDSTUFF僠>僠(.*)");

        runner.enqueue("僠<僠WILDSTUFF僠>僠Hello World\n僠<僠WILDSTUFF僠>僠Goodbye\n僠<僠WILDSTUFF僠>僠I Mean IT!僠<僠WILDSTUFF僠>\n".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PutKafkaExt.REL_SUCCESS, 1);
        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);
        assertEquals("僠<僠WILDSTUFF僠>僠Hello World\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("僠<僠WILDSTUFF僠>僠Goodbye\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("僠<僠WILDSTUFF僠>僠I Mean IT!僠<僠WILDSTUFF僠>\n", new String(consumer.next().message(), StandardCharsets.UTF_8));
        runner.shutdown();
    }



    private ConsumerIterator<byte[], byte[]> buildConsumer(String topic) {
        Properties props = new Properties();
        props.put("zookeeper.connect", "0.0.0.0:" + kafkaLocal.getZookeeperPort());
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
