package org.apache.jackrabbit.oak.store.zeromq;

import junit.framework.TestCase;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class S3BlobStoreAdapterTest extends TestCase {

    @Ignore("For manual testing only")
    @Test
    public void testConnection() throws IOException {
        S3BlobStoreAdapter s3BlobStoreAdapter = new S3BlobStoreAdapter("", "", "", "", "", "");
        BiConsumer<String, InputStream> writer = s3BlobStoreAdapter.getWriter();
        String sourceSentence = "test blob test blob test blob test blob test blob test blob test blob test blob test blob test blob";
        byte[] blob = sourceSentence.getBytes();
        ByteArrayInputStream is = new ByteArrayInputStream(blob);
        writer.accept("simple/test-blob", is);
        assertEquals(0, is.available());
        Function<String, InputStream> reader = s3BlobStoreAdapter.getReader();
        InputStream readerIs = reader.apply("simple/test-blob");
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(readerIs, bos);
        String sentence = new String(bos.toByteArray());
        assertEquals(sourceSentence, sentence);
    }
}