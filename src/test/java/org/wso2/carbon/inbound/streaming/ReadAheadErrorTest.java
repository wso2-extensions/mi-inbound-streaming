/*
 *  Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.inbound.streaming;

import org.junit.Assert;
import org.junit.Test;
import org.wso2.carbon.inbound.streaming.csv.CSVStreamingProcessor;
import org.wso2.carbon.inbound.streaming.json.JSONStreamingProcessor;
import org.wso2.carbon.inbound.streaming.jsonl.JSONLStreamingProcessor;
import org.wso2.carbon.inbound.streaming.text.TextStreamingProcessor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * Every format iterator reads one record ahead. A failure while reading record N+1 must not
 * swallow record N (or the chunk being filled): the iterator returns what it already has, and the
 * following {@code next()} throws the non-recoverable error, numbered at the failing record.
 */
public class ReadAheadErrorTest {

    private static final String JSON_TRUNCATED = "[{\"a\":1},{\"a\":2},{\"a\":";
    private static final String JSON_MISSING_COMMA = "[{\"a\":1},{\"a\":2} {\"a\":3}]";
    private static final String CSV_UNTERMINATED = "id,name\n1,a\n2,b\n3,\"unterminated\n";

    private static InputStream stream(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }

    /** Serves {@code s}, then fails the next read, like a dropped remote connection. */
    private static InputStream failingAfter(String s) {
        final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return new InputStream() {
            private boolean served;

            @Override
            public int read() throws IOException {
                throw new IOException("connection reset");
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (served) {
                    throw new IOException("connection reset");
                }
                served = true;
                int n = Math.min(len, bytes.length);
                System.arraycopy(bytes, 0, b, off, n);
                return n;
            }
        };
    }

    private static StreamingException nextFails(Iterator<?> it) {
        Assert.assertTrue("the pending error keeps the iterator open", it.hasNext());
        try {
            it.next();
        } catch (StreamingException e) {
            Assert.assertFalse(e.isRecoverable());
            Assert.assertFalse("the error is thrown once", it.hasNext());
            return e;
        }
        Assert.fail("expected the read-ahead error");
        return null;
    }

    private static void assertRecords(Iterator<StreamRecord> it, String... expected) {
        for (String content : expected) {
            Assert.assertTrue(it.hasNext());
            Assert.assertEquals(content, it.next().getAsString());
        }
    }

    private static void assertChunk(Iterator<StreamChunk> it, int records) {
        Assert.assertTrue(it.hasNext());
        StreamChunk chunk = it.next();
        Assert.assertEquals(records, chunk.getRecordCount());
        Assert.assertFalse("a chunk followed by an error is not the last", chunk.isLastChunk());
    }

    // ---- JSON ----

    @Test
    public void jsonRecordBeforeTruncationIsReturned() {
        Iterator<StreamRecord> it = new JSONStreamingProcessor(8192, "$[*]", false)
            .getRecordIterator(stream(JSON_TRUNCATED), "application/json");
        assertRecords(it, "{\"a\":1}", "{\"a\":2}");
        StreamingException e = nextFails(it);
        Assert.assertEquals(StreamingException.Kind.PARSE, e.getKind());
        Assert.assertEquals(3, e.getRowNumber());
    }

    @Test
    public void jsonRecordBeforeMissingCommaIsReturned() {
        Iterator<StreamRecord> it = new JSONStreamingProcessor(8192, "$[*]", false)
            .getRecordIterator(stream(JSON_MISSING_COMMA), "application/json");
        assertRecords(it, "{\"a\":1}", "{\"a\":2}");
        Assert.assertEquals(3, nextFails(it).getRowNumber());
    }

    @Test
    public void jsonPartialChunkIsReturnedBeforeError() {
        Iterator<StreamChunk> it = new JSONStreamingProcessor(8192, "$[*]", true)
            .getChunkIterator(stream(JSON_TRUNCATED), "application/json", 5);
        assertChunk(it, 2);
        Assert.assertEquals(3, nextFails(it).getRowNumber());
    }

    @Test
    public void jsonFullChunkThenErrorOnNextChunk() {
        Iterator<StreamChunk> it = new JSONStreamingProcessor(8192, "$[*]", false)
            .getChunkIterator(stream(JSON_TRUNCATED), "application/json", 2);
        assertChunk(it, 2);
        Assert.assertEquals(3, nextFails(it).getRowNumber());
    }

    @Test
    public void jsonErrorOnFirstRecordSurfacesFromNext() {
        Iterator<StreamRecord> it = new JSONStreamingProcessor(8192, "$[*]", false)
            .getRecordIterator(stream("[{\"a\":"), "application/json");
        Assert.assertEquals(1, nextFails(it).getRowNumber());
    }

    // ---- CSV ----

    @Test
    public void csvRecordBeforeBrokenQuoteIsReturned() {
        Iterator<StreamRecord> it = new CSVStreamingProcessor(8192, ',', '"', true, false, false)
            .getRecordIterator(stream(CSV_UNTERMINATED), "text/csv");
        assertRecords(it, "1,a", "2,b");
        Assert.assertEquals(StreamingException.Kind.PARSE, nextFails(it).getKind());
    }

    @Test
    public void csvPartialChunkIsReturnedBeforeError() {
        Iterator<StreamChunk> it = new CSVStreamingProcessor(8192, ',', '"', true, true, false)
            .getChunkIterator(stream(CSV_UNTERMINATED), "text/csv", 5);
        assertChunk(it, 2);
        Assert.assertEquals(StreamingException.Kind.PARSE, nextFails(it).getKind());
    }

    // ---- JSONL (per-line parse errors are recoverable; read failures are not) ----

    @Test
    public void jsonlRecordBeforeReadFailureIsReturned() {
        Iterator<StreamRecord> it = new JSONLStreamingProcessor(8192, false)
            .getRecordIterator(failingAfter("{\"a\":1}\n{\"a\":2}\n"), "application/json");
        assertRecords(it, "{\"a\":1}", "{\"a\":2}");
        StreamingException e = nextFails(it);
        Assert.assertEquals(StreamingException.Kind.IO, e.getKind());
        Assert.assertEquals(3, e.getRowNumber());
    }

    @Test
    public void jsonlPartialChunkIsReturnedBeforeReadFailure() {
        Iterator<StreamChunk> it = new JSONLStreamingProcessor(8192, true)
            .getChunkIterator(failingAfter("{\"a\":1}\n{\"a\":2}\n"), "application/json", 5);
        assertChunk(it, 2);
        Assert.assertEquals(StreamingException.Kind.IO, nextFails(it).getKind());
    }

    // ---- Text (a read failure used to end the file silently, as if at EOF) ----

    @Test
    public void textReadFailureIsNotTreatedAsEndOfFile() {
        Iterator<StreamRecord> it = new TextStreamingProcessor(8192, false)
            .getRecordIterator(failingAfter("one\ntwo\n"), "text/plain");
        assertRecords(it, "one", "two");
        StreamingException e = nextFails(it);
        Assert.assertEquals(StreamingException.Kind.IO, e.getKind());
        Assert.assertEquals(3, e.getRowNumber());
    }

    @Test
    public void textPartialChunkIsReturnedBeforeReadFailure() {
        Iterator<StreamChunk> it = new TextStreamingProcessor(8192, true)
            .getChunkIterator(failingAfter("one\ntwo\n"), "text/plain", 5);
        assertChunk(it, 2);
        Assert.assertEquals(StreamingException.Kind.IO, nextFails(it).getKind());
    }
}
