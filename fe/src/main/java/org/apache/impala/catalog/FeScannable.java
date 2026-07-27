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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.Iterables;

import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.util.ListMap;

/**
 * Interface for tables that support file-based scanning by the planner. Extracted from
 * FeFsTable to decouple scan-path operations from HDFS-specific concerns (caching,
 * write access, file formats).
 *
 * Both FeFsTable (HDFS/Ozone tables) and FeIcebergTable implement this interface,
 * allowing the planner scan infrastructure to work with either without instanceof checks.
 */
public interface FeScannable extends FeTable {

  /** Returns the storage location (path) of this table. */
  String getLocation();

  /**
   * Returns the value Hive is configured to use for NULL partition key values.
   */
  String getNullPartitionKeyValue();

  /** Returns the FsType where files of this table are stored. */
  FileSystemUtil.FsType getFsType();

  /** Returns the total number of bytes stored for this table. */
  long getTotalHdfsBytes();

  /** Returns the index of hosts that store replicas of blocks of this table. */
  ListMap<TNetworkAddress> getHostIndex();

  /** Returns all partitions of this table. */
  Collection<? extends PrunablePartition> getPartitions();

  /** Returns identifiers for all partitions in this table. */
  Set<Long> getPartitionIds();

  /** Returns the map from partition identifier to prunable partition. */
  Map<Long, ? extends PrunablePartition> getPartitionMap();

  /**
   * Returns a map from value to a set of partitions for which column at index 'col'
   * has that value.
   */
  TreeMap<LiteralExpr, Set<Long>> getPartitionValueMap(int col);

  /** Returns the set of partitions which have a null value for column index 'colIdx'. */
  Set<Long> getNullPartitionIds(int colIdx);

  /**
   * Returns the full partition objects for the given partition IDs, which must
   * have been obtained by prior calls to the above methods.
   * @throws IllegalArgumentException if any partition ID does not exist
   */
  List<? extends FeFsPartition> loadPartitions(Collection<Long> ids);

  /** Convenience method to load exactly one partition from a table. */
  default FeFsPartition loadPartition(long partitionId) {
    Collection<? extends FeFsPartition> partCol = loadPartitions(
        Collections.singleton(partitionId));
    if (partCol.size() != 1) {
      throw new AssertionError(String.format(
          "expected exactly one result fetching partition ID %s from table %s " +
              "(got %s)", partitionId, getFullName(), partCol.size()));
    }
    return Iterables.getOnlyElement(partCol);
  }

  /** Load all partitions from the table. */
  default Collection<? extends FeFsPartition> loadAllPartitions() {
    return loadPartitions(getPartitionIds());
  }

  /**
   * Parses and returns the value of the 'skip.header.line.count' table property. If the
   * value is not set for the table, returns 0. If parsing fails or a value < 0 is found,
   * the error parameter is updated to contain an error message.
   */
  int parseSkipHeaderLineCount(StringBuilder error);

  /**
   * Returns the index in the list of sort-by columns for col_name, or -1 if not found.
   */
  int getSortByColumnIndex(String col_name);

  /** Returns true if 'col_name' names the leading sort-by column. */
  default boolean isLeadingSortByColumn(String col_name) {
    return getSortByColumnIndex(col_name) == 0;
  }

  /** Returns true if 'col_name' appears in the list of sort-by columns. */
  default boolean isSortByColumn(String col_name) {
    return getSortByColumnIndex(col_name) >= 0;
  }

  /** Returns the sort order for sort-by columns, or null if none. */
  TSortingOrder getSortOrderForSortByColumn();

  /** Returns true if the sort order for sort-by columns is lexical. */
  default boolean IsLexicalSortByColumn() {
    TSortingOrder sortOrder = getSortOrderForSortByColumn();
    if (sortOrder == null) return false;
    return sortOrder == TSortingOrder.LEXICAL;
  }

  /**
   * Selects a random sample of files from the table such that the sum of file sizes
   * is at least 'percentBytes' percent of the total bytes and at least 'minSampleBytes'.
   * Returns a map from partition id to list of file descriptors.
   */
  Map<Long, List<FileDescriptor>> getFilesSample(
      long percentBytes, long minSampleBytes, long randomSeed);
}
