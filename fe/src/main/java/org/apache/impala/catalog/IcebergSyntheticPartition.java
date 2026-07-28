// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.catalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.Path;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.THdfsPartitionLocation;
import org.apache.impala.thrift.THdfsStorageDescriptor;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.util.ListMap;

import com.google.common.collect.ImmutableList;

/**
 * A lightweight synthetic partition for Iceberg tables. Iceberg tables don't have
 * real HDFS partitions, but the scan infrastructure (HdfsScanNode) and the backend
 * expect exactly one partition to exist in THdfsTable.partitions. This class provides
 * a minimal implementation of FeFsPartition that satisfies those contracts without
 * requiring a full HdfsTable to be loaded.
 *
 * The partition ID is assigned once at construction time and remains stable for the
 * lifetime of this object. It must be consistent between planning (where it goes into
 * scan range splits) and thrift serialization (where it becomes the key in
 * THdfsTable.partitions).
 */
public class IcebergSyntheticPartition implements FeFsPartition {
  private static final AtomicLong ID_COUNTER = new AtomicLong();

  private final long id_;
  private final String location_;
  private final HdfsStorageDescriptor storageDescriptor_;
  private final FileSystemUtil.FsType fsType_;
  private final ListMap<TNetworkAddress> hostIndex_;

  public IcebergSyntheticPartition(String location,
      HdfsFileFormat fileFormat, FileSystemUtil.FsType fsType,
      ListMap<TNetworkAddress> hostIndex) {
    this(ID_COUNTER.getAndIncrement(), location, fileFormat, fsType, hostIndex);
  }

  public IcebergSyntheticPartition(long id, String location,
      HdfsFileFormat fileFormat, FileSystemUtil.FsType fsType,
      ListMap<TNetworkAddress> hostIndex) {
    id_ = id;
    location_ = location;
    fsType_ = fsType;
    hostIndex_ = hostIndex;
    THdfsStorageDescriptor tsd = new THdfsStorageDescriptor(
        (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
        fileFormat.toThrift(), 0);
    storageDescriptor_ = HdfsStorageDescriptor.fromThrift(tsd, "iceberg-synthetic");
  }

  @Override public long getId() { return id_; }

  @Override
  public List<LiteralExpr> getPartitionValues() { return ImmutableList.of(); }

  @Override public String getPartitionName() { return ""; }

  @Override
  public FeFsTable getTable() {
    // Iceberg tables no longer implement FeFsTable. This method is not called
    // for Iceberg synthetic partitions because getDefaultPartialPartitionInfo
    // is overridden below.
    return null;
  }

  @Override
  public ListMap<TNetworkAddress> getHostIndex() { return hostIndex_; }

  @Override
  public FileSystemUtil.FsType getFsType() { return fsType_; }

  @Override
  public List<FileDescriptor> getFileDescriptors() { return Collections.emptyList(); }

  @Override
  public List<FileDescriptor> getInsertFileDescriptors() {
    return Collections.emptyList();
  }

  @Override
  public List<FileDescriptor> getDeleteFileDescriptors() {
    return Collections.emptyList();
  }

  @Override public boolean hasFileDescriptors() { return false; }
  @Override public int getNumFileDescriptors() { return 0; }

  @Override public String getLocation() { return location_; }

  @Override
  public THdfsPartitionLocation getLocationAsThrift() {
    THdfsPartitionLocation loc = new THdfsPartitionLocation();
    loc.setPrefix_index(-1);
    loc.setSuffix(location_);
    return loc;
  }

  @Override
  public Path getLocationPath() { return new Path(location_); }

  @Override
  public TAccessLevel getAccessLevel() { return TAccessLevel.READ_WRITE; }

  @Override public boolean isCacheable() { return false; }
  @Override public boolean isMarkedCached() { return false; }

  @Override
  public HdfsStorageDescriptor getInputFormatDescriptor() { return storageDescriptor_; }

  @Override
  public HdfsFileFormat getFileFormat() { return storageDescriptor_.getFileFormat(); }

  @Override public TPartitionStats getPartitionStats() { return null; }
  @Override public boolean hasIncrementalStats() { return false; }
  @Override public byte[] getPartitionStatsCompressed() { return null; }
  @Override public long getSize() { return 0; }
  @Override public long getNumRows() { return -1; }

  @Override
  public LiteralExpr getPartitionValue(int pos) {
    throw new IndexOutOfBoundsException(
        "Iceberg tables have no partition key columns at the HDFS level");
  }

  @Override
  public Map<String, String> getParameters() { return Collections.emptyMap(); }

  @Override public long getWriteId() { return -1; }
  @Override public FeFsPartition genInsertDeltaPartition() { return this; }
  @Override public FeFsPartition genDeleteDeltaPartition() { return this; }

  @Override
  public TPartialPartitionInfo getDefaultPartialPartitionInfo(
      TGetPartialCatalogObjectRequest req) {
    TPartialPartitionInfo partInfo = new TPartialPartitionInfo(getId());
    if (req.table_info_selector.want_partition_names) {
      partInfo.setName("");
    }
    if (req.table_info_selector.want_partition_metadata) {
      partInfo.setHas_incremental_stats(false);
    }
    if (req.table_info_selector.want_partition_files) {
      partInfo.setLast_compaction_id(-1);
      partInfo.insert_file_descriptors = new ArrayList<>();
      partInfo.delete_file_descriptors = new ArrayList<>();
      partInfo.file_descriptors = new ArrayList<>();
    }
    if (req.table_info_selector.want_partition_stats) {
      partInfo.setPartition_stats((byte[]) null);
    }
    partInfo.setIs_marked_cached(false);
    return partInfo;
  }
}
