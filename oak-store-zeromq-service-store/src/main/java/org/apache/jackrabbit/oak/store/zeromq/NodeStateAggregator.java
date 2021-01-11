package org.apache.jackrabbit.oak.store.zeromq;

public interface NodeStateAggregator extends Runnable {
    boolean hasCaughtUp();

    ZeroMQNodeStore getNodeStore();

    String getJournalHead(String journalName);
}
