package org.apache.jackrabbit.oak.store.zeromq;

import java.io.IOException;
import java.io.OutputStream;

public interface TemporaryBlob {
  OutputStream getOutputStream() throws IOException;
  void delete() throws IOException;
}
