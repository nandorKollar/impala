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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.THdfsPartitionLocation;
import org.apache.impala.thrift.THdfsStorageDescriptor;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.ListMap;

/**
 * Lightweight synthetic FeFsPartition for Iceberg tables. Iceberg tables present a
 * single "partition" to the HdfsScanNode infrastructure — actual file metadata comes
 * from IcebergContentFileStore, not from HdfsPartition. This class provides just enough
 * to satisfy HdfsScanNode's constructor and the methods it calls on partition objects.
 */
public class IcebergSyntheticPartition implements FeFsPartition {
  private final FeIcebergTable table_;
  private final HdfsFileFormat fileFormat_;

  public IcebergSyntheticPartition(FeIcebergTable table) {
    table_ = table;
    fileFormat_ = IcebergUtil.toHdfsFileFormat(table.getIcebergFileFormat());
  }

  @Override
  public long getId() { return FeIcebergTable.ICEBERG_PARTITION_ID; }

  @Override
  public List<LiteralExpr> getPartitionValues() { return Collections.emptyList(); }

  @Override
  public String getPartitionName() { return ""; }

  @Override
  public FeFsTable getTable() { return table_; }

  @Override
  public ListMap<TNetworkAddress> getHostIndex() { return table_.getHostIndex(); }

  @Override
  public FileSystemUtil.FsType getFsType() { return table_.getFsType(); }

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

  @Override
  public boolean hasFileDescriptors() { return false; }

  @Override
  public int getNumFileDescriptors() { return 0; }

  @Override
  public String getLocation() { return table_.getLocation(); }

  @Override
  public THdfsPartitionLocation getLocationAsThrift() {
    throw new UnsupportedOperationException(
        "IcebergSyntheticPartition does not support getLocationAsThrift");
  }

  @Override
  public Path getLocationPath() { return new Path(table_.getLocation()); }

  @Override
  public TAccessLevel getAccessLevel() { return TAccessLevel.READ_WRITE; }

  @Override
  public boolean isCacheable() { return false; }

  @Override
  public boolean isMarkedCached() { return false; }

  @Override
  public HdfsStorageDescriptor getInputFormatDescriptor() {
    THdfsStorageDescriptor tDesc = new THdfsStorageDescriptor(
        (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
        IcebergUtil.toTHdfsFileFormat(table_.getIcebergFileFormat()), 0);
    return HdfsStorageDescriptor.fromThrift(tDesc, table_.getFullName());
  }

  @Override
  public HdfsFileFormat getFileFormat() { return fileFormat_; }

  @Override
  public TPartitionStats getPartitionStats() { return null; }

  @Override
  public boolean hasIncrementalStats() { return false; }

  @Override
  public byte[] getPartitionStatsCompressed() { return null; }

  @Override
  public long getSize() { return -1; }

  @Override
  public long getNumRows() { return -1; }

  @Override
  public LiteralExpr getPartitionValue(int pos) {
    throw new IndexOutOfBoundsException(
        "IcebergSyntheticPartition has no partition values");
  }

  @Override
  public Map<String, String> getParameters() { return Collections.emptyMap(); }

  @Override
  public long getWriteId() { return -1; }

  @Override
  public FeFsPartition genInsertDeltaPartition() {
    throw new UnsupportedOperationException(
        "IcebergSyntheticPartition does not support ACID deltas");
  }

  @Override
  public FeFsPartition genDeleteDeltaPartition() {
    throw new UnsupportedOperationException(
        "IcebergSyntheticPartition does not support ACID deltas");
  }

  @Override
  public String toString() {
    return "IcebergSyntheticPartition(id=" + getId() + ", format=" + fileFormat_ + ")";
  }
}
