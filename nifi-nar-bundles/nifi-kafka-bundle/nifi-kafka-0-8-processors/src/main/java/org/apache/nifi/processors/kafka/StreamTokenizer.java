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

import java.io.*;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The <code>StreamTokenizer</code> class takes an input stream and demarcates
 * it so it could be read (see {@link #nextToken()}) as individual byte[]
 * demarcated by the provided regex delimiter. If delimiter is not provided the entire
 * stream will be read into a single token which may result in
 * {@link OutOfMemoryError} if stream is too large.
 */
public class StreamTokenizer {

    private final static int INIT_BUFFER_SIZE = 8192;

    private final BufferedReader bufferedReader;

    private final Pattern patternDelimiter;

    private final static String DEFAULT_REGEX = ".*";

    private final static String DEFAULT_CHARSET = "UTF-8";

    private String currentLine = "";

    private StringBuilder buffer = null;

    private long blocsCount = 0L;

    private long bytesOfLineSeparator;
    private boolean isLeadingBloc = true;

    private boolean endOfStream = false;


    public StreamTokenizer(InputStream is) {
        this(is, DEFAULT_REGEX, DEFAULT_CHARSET);
    }

    /**
     * Constructs a new instance
     *
     * @param is             instance of {@link InputStream} representing the data
     * @param regexDelimiter String representing delimiter regex used to split the
     *                       input stream. Can be null
     */
    public StreamTokenizer(InputStream is, String regexDelimiter, String charset) {
        this.validateInput(is, regexDelimiter);
        this.bufferedReader = new BufferedReader(new InputStreamReader(is));
        this.patternDelimiter = Pattern.compile(regexDelimiter);
        try {
            this.bytesOfLineSeparator = "\n".getBytes(charset).length;
        } catch (UnsupportedEncodingException e) {
            this.bytesOfLineSeparator = "\n".getBytes(Charset.defaultCharset()).length;
        }

    }

    /**
     * Will read the next data token from the {@link InputStream} returning null
     * when it reaches the end of the stream.
     */
    public byte[] nextToken() {

        Matcher matcher;


        try {
            currentLine = bufferedReader.readLine();

            while (currentLine != null) {
                matcher = patternDelimiter.matcher(currentLine);
                if (matcher.matches()) {
                    blocsCount += 1;
                    if (isLeadingBloc) {
                        isLeadingBloc = false;

                        buffer = new StringBuilder(currentLine + "\n");

                    } else {
                        final byte[] bloc = buffer.toString().getBytes();

                        buffer = new StringBuilder(currentLine + "\n");
                        return bloc;
                    }

                } else if (buffer != null) {
                    buffer.append(currentLine).append("\n");
                } else {
                    buffer = new StringBuilder(currentLine + "\n");
                }
                currentLine = bufferedReader.readLine();
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

        if (blocsCount == 0 || endOfStream) {
            return null;
        } else {
            endOfStream = true;
            return buffer.toString().getBytes();
        }


    }


    /**
     *
     */
    private void validateInput(InputStream is, String regexDelimiter) {
        if (is == null) {
            throw new IllegalArgumentException("'is' must not be null");
        }
    }
}
