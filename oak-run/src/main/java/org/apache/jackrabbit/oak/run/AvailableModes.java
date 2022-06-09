/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.run;

import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.copy.CopyCommand;
import org.apache.jackrabbit.oak.exporter.NodeStateExportCommand;
import org.apache.jackrabbit.oak.index.IndexCommand;
import org.apache.jackrabbit.oak.index.merge.IndexDiffCommand;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.Modes;
import org.apache.jackrabbit.oak.simple.CommHubCommand;
import org.apache.jackrabbit.oak.simple.ImportToSimpleCommand;
import org.apache.jackrabbit.oak.simple.SerialiseNodeStoreCommand;
import org.apache.jackrabbit.oak.simple.SimpleBlobReaderServiceCommand;
import org.apache.jackrabbit.oak.simple.SimpleNodeStateWriterServiceCommand;
import org.apache.jackrabbit.oak.simple.SimpleQueueListenerCommand;

public final class AvailableModes {
    // list of available Modes for the tool
    public static final Modes MODES = new Modes(
        ImmutableMap.<String, Command>builder()
            .put("backup", new BackupCommand())
            .put("check", new CheckCommand())
            .put("checkpoints", new CheckpointsCommand())
            .put("clusternodes", new ClusterNodesCommand())
            .put("compact", new CompactCommand())
            .put("composite-prepare", new CompositePrepareCommand())
            .put("console", new ConsoleCommand())
            .put("copy", new CopyCommand())
            .put(DataStoreCommand.NAME, new DataStoreCommand())
            .put(DataStoreCopyCommand.NAME, new DataStoreCopyCommand())
            .put("datastorecacheupgrade", new DataStoreCacheUpgradeCommand())
            .put("datastorecheck", new DataStoreCheckCommand())
            .put("debug", new DebugCommand())
            .put(DocumentStoreCheckCommand.NAME, new DocumentStoreCheckCommand())
            .put("explore", new ExploreCommand())
            .put(NodeStateExportCommand.NAME, new NodeStateExportCommand())
            .put(FrozenNodeRefsByScanningCommand.NAME, new FrozenNodeRefsByScanningCommand())
            .put(FrozenNodeRefsUsingIndexCommand.NAME, new FrozenNodeRefsUsingIndexCommand())
            .put("garbage", new GarbageCommand())
            .put("help", new HelpCommand())
            .put("history", new HistoryCommand())
            .put("import-to-simple", new ImportToSimpleCommand())
            .put("index-merge", new IndexMergeCommand())
            .put("index-diff", new IndexDiffCommand())
            .put(IndexCommand.NAME, new IndexCommand())
            .put(IOTraceCommand.NAME, new IOTraceCommand())
            .put(JsonIndexCommand.INDEX, new JsonIndexCommand())
            .put(PersistentCacheCommand.PERSISTENTCACHE, new PersistentCacheCommand())
            .put("rdbddldump", new RDBDDLDumpCommand())
            .put("recovery", new RecoveryCommand())
            .put("recover-journal", new RecoverJournalCommand())
            .put("revisions", new RevisionsCommand())
            .put("repair", new RepairCommand())
            .put("resetclusterid", new ResetClusterIdCommand())
            .put("restore", new RestoreCommand())
            .put(SerialiseNodeStoreCommand.NAME, new SerialiseNodeStoreCommand())
            .put("simple-blob-reader-service", new SimpleBlobReaderServiceCommand())
            .put("simple-blob-writer-service", new SimpleNodeStateWriterServiceCommand())
            .put(CommHubCommand.NAME, new CommHubCommand())
            .put(SimpleQueueListenerCommand.NAME, new SimpleQueueListenerCommand())
            .put("tarmkdiff", new FileStoreDiffCommand())
            .put(ThreadDumpCommand.THREADDUMP, new ThreadDumpCommand())
            .put("tika", new TikaCommand())
            .put("unlockupgrade", new UnlockUpgradeCommand())
            .put("upgrade", new UpgradeCommand())
            .put("search-nodes", new SearchNodesCommand())
            .put("segment-copy", new SegmentCopyCommand())
            .put("server", new ServerCommand())
            .put("purge-index-versions", new LucenePurgeOldIndexVersionCommand())
            .build());
}
