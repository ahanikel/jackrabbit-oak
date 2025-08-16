package org.apache.jackrabbit.oak.store.zeromq;

public class Constants {
  public static final String PARAM_JOURNAL_ID = "journalId";
  public static final String PARAM_BACKEND_READER_URL = "backendReaderURL";
  public static final String PARAM_BACKEND_WRITER_URL = "backendWriterURL";
  public static final String PARAM_INIT_JOURNAL = "initJournal";
  public static final String PARAM_BLOB_CACHE_DIR = "blobCacheDir";

  public static final String DEFAULT_JOURNAL_ID = "golden";
  public static final String DEFAULT_BACKEND_READER_URL = "tcp://localhost:8000";
  public static final String DEFAULT_BACKEND_WRITER_URL = "tcp://localhost:8001";
  public static final String DEFAULT_BLOB_CACHE_DIR = "/tmp/blobCacheDir";
}
