package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Blob;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public interface NodeStateAggregator extends Runnable, Closeable {
    boolean hasCaughtUp();
    String getJournalHead(String instanceName);
    String readNodeState(String msg);
    FileInputStream getBlob(String reference) throws FileNotFoundException;
}
