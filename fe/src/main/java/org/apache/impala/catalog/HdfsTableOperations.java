package org.apache.impala.catalog;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.FsPermissionCache;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.impala.util.ThreadNameAnnotator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class HdfsTableOperations {

    /**
     * See 'HdfsTableLoadParams' class for the argument list passed into this method.
     * Loads table metadata from the Hive Metastore.
     * <p>
     * If 'reuseMetadata' is false, performs a full metadata load from the Hive Metastore,
     * including partition and file metadata. Otherwise, loads metadata incrementally and
     * updates this HdfsTable in place so that it is in sync with the Hive Metastore.
     * <p>
     * Depending on the operation that triggered the table metadata load, not all the
     * metadata may need to be updated. If 'partitionsToUpdate' is not null, it specifies a
     * list of partitions for which metadata should be updated. Otherwise, all partition
     * metadata will be updated from the Hive Metastore.
     * <p>
     * If 'loadParitionFileMetadata' is true, file metadata of the specified partitions
     * are reloaded from scratch. If 'partitionsToUpdate' is not specified, file metadata
     * of all the partitions are loaded.
     * <p>
     * If 'loadTableSchema' is true, the table schema is loaded from the Hive Metastore.
     * <p>
     * If 'isPreLoadForInsert' is true, then we intend to refresh partitions from the Hive
     * Metastore without reloading the file metadata(this is done in later steps) to ensure
     * consistency while inserting into partitioned tables.
     * <p>
     * Existing file descriptors might be reused incorrectly if Hdfs rebalancer was
     * executed, as it changes the block locations but doesn't update the mtime (file
     * modification time).
     * If this occurs, user has to execute "invalidate metadata" to invalidate the
     * metadata cache of the table and trigger a fresh load.
     */
//    public void load(Table table, HdfsTableLoadParams loadParams) throws TableLoadingException {
//        final Timer.Context context =
//                table.getMetrics().getTimer(Table.LOAD_DURATION_METRIC).time();
//        IMetaStoreClient msClient = loadParams.getMsClient();
//        org.apache.hadoop.hive.metastore.api.Table msTbl = loadParams.getMsTable();
//        EventSequence catalogTimeline = loadParams.getCatalogTimeline();
//        Set<String> partitionsToUpdate = loadParams.getPartitionsToUpdate();
//        String annotation = String.format("%s metadata for %s%s partition(s) of %s.%s (%s)",
//                loadParams.getIsReuseMetadata() ? "Reloading" : "Loading",
//                loadParams.getLoadTableSchema() ? "table definition and " : "",
//                partitionsToUpdate == null ? "all" : String.valueOf(partitionsToUpdate.size()),
//                msTbl.getDbName(), msTbl.getTableName(),
//                loadParams.getReason());
//        LOG.info(annotation);
//        final Timer storageLdTimer =
//                table.getMetrics().getTimer(Table.LOAD_DURATION_STORAGE_METADATA);
//        storageMetadataLoadTime_ = 0;
//        Table.LOADING_TABLES.incrementAndGet();
//        try (ThreadNameAnnotator tna = new ThreadNameAnnotator(annotation)) {
//            // turn all exceptions into TableLoadingException
//            msTable_ = msTbl;
//            try {
//                if (loadParams.getLoadTableSchema()) {
//                    // set nullPartitionKeyValue from the hive conf.
//                    nullPartitionKeyValue_ =
//                            MetaStoreUtil.getNullPartitionKeyValue(msClient).intern();
//                    loadSchema(msTbl);
//                    loadAllColumnStats(msClient, catalogTimeline);
//                    loadConstraintsInfo(msClient, msTbl);
//                    catalogTimeline.markEvent("Loaded table schema");
//                }
//                boolean prevWriteIdChanged = loadValidWriteIdList(msClient);
//                if (prevWriteIdChanged && !loadParams.isLoadPartitionFileMetadata()) {
//                    LOG.info("Not skipping file metadata reload since writeId is changed in the " +
//                            "metastore for the table: " + getTableName());
//                }
//                // Set table-level stats first so partition stats can inherit it.
//                setTableStats(msTbl);
//                // Load partition and file metadata
//                if (loadParams.getIsReuseMetadata()) {
//                    // Incrementally update this table's partitions and file metadata
//                    if (!loadParams.getIsPreLoadForInsert()) {
//                        Preconditions.checkState(partitionsToUpdate == null ||
//                                loadParams.isLoadPartitionFileMetadata(), "Conflicts in " +
//                                "'partitionsToUpdate' and 'loadPartitionFileMetadata'");
//                    }
//                    storageMetadataLoadTime_ += updateMdFromHmsTable(msTbl);
//                    if (msTbl.getPartitionKeysSize() == 0) {
//                        if (loadParams.isLoadPartitionFileMetadata() || prevWriteIdChanged) {
//                            storageMetadataLoadTime_ += updateUnpartitionedTableFileMd(
//                                    msClient, loadParams.getDebugAction(),
//                                    catalogTimeline);
//                        } else {  // Update the single partition stats in case table stats changes.
//                            updateUnpartitionedTableStats();
//                        }
//                    } else {
//                        storageMetadataLoadTime_ += updatePartitionsFromHms(msClient,
//                                partitionsToUpdate, loadParams.isLoadPartitionFileMetadata(),
//                                loadParams.getRefreshUpdatedPartitions(),
//                                loadParams.getPartitionToEventId(), loadParams.getDebugAction(),
//                                catalogTimeline, loadParams.getIsPreLoadForInsert());
//                    }
//                    LOG.info("Incrementally loaded table metadata for: " + getTableName());
//                } else {
//                    LOG.info("Fetching partition metadata from the Metastore: " + getTableName());
//                    final Timer.Context allPartitionsLdContext =
//                            getMetrics().getTimer(HdfsTable.LOAD_DURATION_ALL_PARTITIONS).time();
//                    // Load all partitions from Hive Metastore, including file metadata.
//                    List<Partition> msPartitions =
//                            MetaStoreUtil.fetchAllPartitions(msClient,
//                                    msTbl, NUM_PARTITION_FETCH_RETRIES);
//                    LOG.info("Fetched partition metadata from the Metastore: " + getTableName());
//                    storageMetadataLoadTime_ = loadAllPartitions(msClient,
//                            msPartitions, msTbl, catalogTimeline);
//                    allPartitionsLdContext.stop();
//                }
//                if (loadParams.getLoadTableSchema()) {
//                    setAvroSchema(msClient, msTbl, catalogTimeline);
//                }
//                updateMetrics();
//                refreshLastUsedTime();
//                // Make sure all the partition modifications are done.
//                Preconditions.checkState(dirtyPartitions_.isEmpty());
//            } catch (TableLoadingException e) {
//                throw e;
//            } catch (Exception e) {
//                throw new TableLoadingException("Failed to load metadata for table: "
//                        + getTableName(), e);
//            }
//        } finally {
//            storageLdTimer.update(storageMetadataLoadTime_, TimeUnit.NANOSECONDS);
//            long load_time_duration = context.stop();
//            if (load_time_duration > LOADING_WARNING_TIME_NS) {
//                LOG.warn("Time taken on loading table " + getTableName() + " exceeded " +
//                        "warning threshold. Time: " + PrintUtils.printTimeNs(load_time_duration));
//            }
//            updateTableLoadingTime();
//            Table.LOADING_TABLES.decrementAndGet();
//        }
//    }
//
//     /**
//     * Load Primary Key and Foreign Key information for table. Throws TableLoadingException
//     * if the load fails. Declared as protected to allow third party extensions on this
//     * class.
//     */
//    protected void loadConstraintsInfo(IMetaStoreClient client,
//                                       org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException{
//        try {
//            sqlConstraints_ = new SqlConstraints(client.getPrimaryKeys(
//                    new PrimaryKeysRequest(msTbl.getDbName(), msTbl.getTableName())),
//                    client.getForeignKeys(new ForeignKeysRequest(null, null,
//                            msTbl.getDbName(), msTbl.getTableName())));
//        } catch (Exception e) {
//            throw new TableLoadingException("Failed to load primary keys/foreign keys for "
//                    + "table: " + getTableName(), e);
//        }
//    }
//
//    /**
//     * Updates the table metadata, including 'hdfsBaseDir_', 'isMarkedCached_',
//     * and 'accessLevel_' from 'msTbl'. Returns time spent accessing file system
//     * in nanoseconds. Throws an IOException if there was an error accessing
//     * the table location path.
//     * Declared as protected to allow third party extension visibility.
//     */
//    protected long updateMdFromHmsTable(org.apache.hadoop.hive.metastore.api.Table msTbl)
//            throws IOException {
//        Preconditions.checkNotNull(msTbl);
//        final Clock clock = Clock.defaultClock();
//        long filesystemAccessTime = 0;
//        long startTime = clock.getTick();
//        hdfsBaseDir_ = msTbl.getSd().getLocation();
//        isMarkedCached_ = HdfsCachingUtil.validateCacheParams(msTbl.getParameters());
//        Path location = new Path(hdfsBaseDir_);
//        accessLevel_ = getAvailableAccessLevel(getTableName().fullName(), location,
//                new FsPermissionCache());
//        filesystemAccessTime = clock.getTick() - startTime;
//        setMetaStoreTable(msTbl);
//        return filesystemAccessTime;
//    }
//
//    /**
//     * Incrementally updates the file metadata of an unpartitioned HdfsTable.
//     * Returns time spent updating the file metadata in nanoseconds.
//     *
//     * This is optimized for the case where few files have changed. See
//     * {@link FileMetadataLoader#load} for details.
//     */
//    private long updateUnpartitionedTableFileMd(IMetaStoreClient client, String debugAction,
//                                                EventSequence catalogTimeline) throws CatalogException {
//        Preconditions.checkState(getNumClusteringCols() == 0);
//        if (LOG.isTraceEnabled()) {
//            LOG.trace("update unpartitioned table: " + getTableName());
//        }
//        // Step 1: fetch external metadata
//        HdfsPartition oldPartition = Iterables.getOnlyElement(partitionMap_.values());
//        org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable();
//        Preconditions.checkNotNull(msTbl);
//        HdfsPartition.Builder partBuilder = createPartitionBuilder(msTbl.getSd(),
//                /*msPartition=*/null, new FsPermissionCache());
//        // Copy over the FDs from the old partition to the new one, so that
//        // 'refreshPartitionFileMetadata' below can compare modification times and
//        // reload the locations only for those that changed.
//        partBuilder.setFileDescriptors(oldPartition);
//        partBuilder.setIsMarkedCached(isMarkedCached_);
//        // Keep track of the previous partition id, so we can send invalidation on the old
//        // partition instance to local catalog coordinators.
//        partBuilder.setPrevId(oldPartition.getId());
//        long fileMdLoadTime = loadFileMetadataForPartitions(client,
//                ImmutableList.of(partBuilder), /*isRefresh=*/true, debugAction, catalogTimeline);
//        // Step 2: update internal fields
//        resetPartitions();
//        setPrototypePartition(msTbl.getSd());
//        setUnpartitionedTableStats(partBuilder);
//        addPartition(partBuilder.build());
//        return fileMdLoadTime;
//    }
//
//    /**
//     * Updates the single partition stats of an unpartitioned HdfsTable.
//     */
//    private void updateUnpartitionedTableStats() throws CatalogException {
//        // Just update the single partition if its #rows is stale.
//        HdfsPartition oldPartition = Iterables.getOnlyElement(partitionMap_.values());
//        if (oldPartition.getNumRows() != getNumRows()) {
//            HdfsPartition.Builder partBuilder = new HdfsPartition.Builder(oldPartition)
//                    .setNumRows(getNumRows());
//            updatePartition(partBuilder);
//        }
//    }
//
//    /**
//     * Updates the partitions of an HdfsTable so that they are in sync with the
//     * Hive Metastore. It reloads partitions that were marked 'dirty' by doing a
//     * DROP + CREATE. It removes from this table partitions that no longer exist
//     * in the Hive Metastore and adds partitions that were added externally (e.g.
//     * using Hive) to the Hive Metastore but do not exist in this table. If
//     * 'loadParitionFileMetadata' is true, it triggers file/block metadata reload
//     * for the partitions specified in 'partitionsToUpdate', if any, or for all
//     * the table partitions if 'partitionsToUpdate' is null. Returns time
//     * spent loading file metadata in nanoseconds.
//     */
//    private long updatePartitionsFromHms(IMetaStoreClient client,
//                                         Set<String> partitionsToUpdate, boolean loadPartitionFileMetadata,
//                                         boolean refreshUpdatedPartitions, Map<String, Long> partitionToEventId,
//                                         String debugAction, EventSequence catalogTimeline, boolean isPreLoadForInsert)
//            throws Exception {
//        if (LOG.isTraceEnabled()) LOG.trace("Sync table partitions: " + getTableName());
//        org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable();
//        Preconditions.checkNotNull(msTbl);
//        Preconditions.checkState(msTbl.getPartitionKeysSize() != 0);
//        if (!isPreLoadForInsert) {
//            Preconditions.checkState(loadPartitionFileMetadata || partitionsToUpdate == null);
//        }
//        HdfsTable.PartitionDeltaUpdater deltaUpdater;
//        if (refreshUpdatedPartitions) {
//            deltaUpdater = new HdfsTable.PartBasedDeltaUpdater(client,
//                    loadPartitionFileMetadata, partitionsToUpdate, partitionToEventId,
//                    debugAction, catalogTimeline);
//        } else {
//            deltaUpdater = new HdfsTable.PartNameBasedDeltaUpdater(client, loadPartitionFileMetadata,
//                    partitionsToUpdate, partitionToEventId, debugAction, catalogTimeline);
//        }
//        deltaUpdater.apply();
//        return deltaUpdater.loadTimeForFileMdNs_;
//    }
}
