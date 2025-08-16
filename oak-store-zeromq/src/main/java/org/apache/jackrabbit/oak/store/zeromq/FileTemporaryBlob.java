package org.apache.jackrabbit.oak.store.zeromq;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class FileTemporaryBlob implements TemporaryBlob {
  private final File file;
  private FileOutputStream fos;

  public FileTemporaryBlob(File file) {
    this.file = file;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    if (fos == null) {
      fos = new FileOutputStream(file);
    }
    return fos;
  }

  public File getFile() {
    return file;
  }

  @Override
  public void delete() {
    try {
      fos.close();
    } catch (IOException e) {
    }
    file.delete();
  }
}
