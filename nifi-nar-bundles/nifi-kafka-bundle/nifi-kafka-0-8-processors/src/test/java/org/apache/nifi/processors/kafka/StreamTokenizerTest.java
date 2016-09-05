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
import org.apache.nifi.stream.io.util.StreamDemarcator;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.*;

// The test is valid and should be ran when working on this module. @Ignore is
// to speed up the overall build
public class StreamTokenizerTest {

    @Test
    public void validateRegexMatch() {
        String data = "[USR-BACK] Be not afraid of greatness:\n" +
                "[USR-BACK] some are born great, some achieve greatness, and some have greatness thrust upon them.\n" +
                "The course of true love never did run smooth.\n" +
                "[USR-BACK] To thine own self be true, \n" +
                "and it must follow, \n" +
                "as the night the day, \n" +
                "thou canst not then be false to any man.\n";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamTokenizer scanner = null;
        try {
            scanner = new StreamTokenizer(is, "(\\[\\S*\\])\\s+(.*)", "UTF-8", 1000, 2);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        assertTrue(Arrays.equals("[USR-BACK] Be not afraid of greatness:\n".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals("[USR-BACK] some are born great, some achieve greatness, and some have greatness thrust upon them.\nThe course of true love never did run smooth.\n".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertTrue(Arrays.equals("[USR-BACK] To thine own self be true, \nand it must follow, \nas the night the day, \nthou canst not then be false to any man.\n".getBytes(StandardCharsets.UTF_8), scanner.nextToken()));
        assertNull(scanner.nextToken());
    }


    @Test
    public void validateRegexUnmatch() {
        String data = "[USR-BACK] Be not afraid of greatness:\n" +
                "[USR-BACK] some are born great, some achieve greatness, and some have greatness thrust upon them.\n" +
                "The course of true love never did run smooth.\n" +
                "[USR-BACK] To thine own self be true, \n" +
                "and it must follow, \n" +
                "as the night the day, \n" +
                "thou canst not then be false to any man.\n";
        ByteArrayInputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        StreamTokenizer scanner = null;
        try {
            scanner = new StreamTokenizer(is, "\\s(\\[\\S*\\])\\s+(.*)", "UTF-8", 1000, 2);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        assertNull(scanner.nextToken());
    }
}
