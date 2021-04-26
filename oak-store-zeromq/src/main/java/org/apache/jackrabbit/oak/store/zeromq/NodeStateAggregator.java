package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Blob;

public interface NodeStateAggregator extends Runnable {
    boolean hasCaughtUp();
    String getJournalHead(String instanceName);
    String readNodeState(String msg);
    Blob getBlob(String reference);
}
