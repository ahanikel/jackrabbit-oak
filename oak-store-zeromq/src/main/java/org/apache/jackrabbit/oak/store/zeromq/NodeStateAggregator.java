package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Blob;

import java.io.Closeable;

public interface NodeStateAggregator extends Runnable, Closeable {
    boolean hasCaughtUp();
    String getJournalHead(String instanceName);
    String readNodeState(String msg);
    Blob getBlob(String reference);
}
