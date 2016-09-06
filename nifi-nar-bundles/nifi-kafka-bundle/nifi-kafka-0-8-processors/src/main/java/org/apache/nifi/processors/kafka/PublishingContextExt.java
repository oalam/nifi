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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Holder of context information used by {@link KafkaPublisher} required to
 * publish messages to Kafka to a random partition.
 */
class PublishingContextExt {

    private final InputStream contentStream;

    private final String topic;

    private final int lastAckedMessageIndex;

    private final int numberOfPartitions;

    private final Random random = new Random();

    private String regexDelimiter = ".*";
    private String charset = "UTF-8";
    /*
     * We're using the default value from Kafka. We are using it to control the
     * message size before it goes to to Kafka thus limiting possibility of a
     * late failures in Kafka client.
     */
    private volatile int maxRequestSize = 1048576; // kafka default

    private volatile boolean maxRequestSizeSet;

    private volatile byte[] keyBytes;

    private volatile byte[] delimiterBytes;

    PublishingContextExt(InputStream contentStream, String topic) {
        this(contentStream, topic, -1, 1);
    }

    PublishingContextExt(InputStream contentStream, String topic, int lastAckedMessageIndex) {
        this(contentStream, topic, lastAckedMessageIndex, 1);
    }

    PublishingContextExt(InputStream contentStream, String topic, int lastAckedMessageIndex, int numberOfPartitions) {
        this.validateInput(contentStream, topic, lastAckedMessageIndex, numberOfPartitions);
        this.contentStream = contentStream;
        this.topic = topic;
        this.lastAckedMessageIndex = lastAckedMessageIndex;
        this.numberOfPartitions = numberOfPartitions;
    }


    public void setCharset(String charset) {
        this.charset = charset;
    }

    public void setRegexDelimiter(String regexDelimiter) {
        this.regexDelimiter = regexDelimiter;
    }

    void setKeyBytes(byte[] keyBytes) {
        if (this.keyBytes == null) {
            if (keyBytes != null) {
                this.assertBytesValid(keyBytes);
                this.keyBytes = keyBytes;
            }
        } else {
            throw new IllegalArgumentException("'keyBytes' can only be set once per instance");
        }
    }

    byte[] getKeyBytes() {
        return this.keyBytes;
    }

    @Override
    public String toString() {
        return "topic: '" + this.topic + "'; delimiter: '" + new String(this.delimiterBytes, StandardCharsets.UTF_8) + "'";
    }

    int getLastAckedMessageIndex() {
        return this.lastAckedMessageIndex;
    }

    Integer getPartitionId() {
        return this.random.nextInt(numberOfPartitions);
    }

    String getRegexDelimiter() {
        return this.regexDelimiter;
    }

    int getNumberOfPartitions() {
        return this.numberOfPartitions;
    }

    public String getCharset() {
        return this.charset;
    }

    InputStream getContentStream() {
        return this.contentStream;
    }

    String getTopic() {
        return this.topic;
    }


    private void assertBytesValid(byte[] bytes) {
        if (bytes != null) {
            if (bytes.length == 0) {
                throw new IllegalArgumentException("'bytes' must not be empty");
            }
        }
    }

    private void validateInput(InputStream contentStream, String topic, int lastAckedMessageIndex, int numberOfPartitions) {
        if (contentStream == null) {
            throw new IllegalArgumentException("'contentStream' must not be null");
        } else if (topic == null || topic.trim().length() == 0) {
            throw new IllegalArgumentException("'topic' must not be null or empty");
        } else if (lastAckedMessageIndex < -1) {
            throw new IllegalArgumentException("'lastAckedMessageIndex' must be >= -1");
        } else if (numberOfPartitions < 0) {
            throw new IllegalArgumentException("'numberOfPartitions' must be >= 0");
        }
    }
}
