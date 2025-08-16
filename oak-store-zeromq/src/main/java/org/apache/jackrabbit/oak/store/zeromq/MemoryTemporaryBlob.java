package org.apache.jackrabbit.oak.store.zeromq;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

public class MemoryTemporaryBlob implements TemporaryBlob {
    private ByteArrayOutputStream data;

    public MemoryTemporaryBlob() {
        this.data = new ByteArrayOutputStream();
    }

    @Override
    public OutputStream getOutputStream() {
      return data;
    }

    @Override
    public void delete() {
      data = new ByteArrayOutputStream();
    }
}
