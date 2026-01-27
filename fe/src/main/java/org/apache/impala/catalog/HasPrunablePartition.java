package org.apache.impala.catalog;

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

import com.google.common.collect.Iterables;
import org.apache.impala.analysis.LiteralExpr;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public interface HasPrunablePartition extends FeTable {
    /**
     * @return all partitions of this table
     */
    Collection<? extends PrunablePartition> getPartitions();

    /**
     * Returns the map from partition identifier to prunable partition.
     */
    Map<Long, ? extends PrunablePartition> getPartitionMap();

    /**
     * @return identifiers for all partitions in this table
     */
    public Set<Long> getPartitionIds();

    /**
     * @param col the index of the target partitioning column
     * @return a map from value to a set of partitions for which column 'col'
     * has that value.
     */
    TreeMap<LiteralExpr, Set<Long>> getPartitionValueMap(int col);

    /**
     * @return the set of partitions which have a null value for column
     * index 'colIdx'.
     */
    Set<Long> getNullPartitionIds(int colIdx);

    /**
     * Returns the full partition objects for the given partition IDs, which must
     * have been obtained by prior calls to the above methods.
     * @throws IllegalArgumentException if any partition ID does not exist
     */
    List<? extends FeFsPartition> loadPartitions(Collection<Long> ids);

    /**
     * Load all partitions from the table.
     */
    default Collection<? extends FeFsPartition> loadAllPartitions() {
        return loadPartitions(getPartitionIds());
    }

    /**
     * Convenience method to load exactly one partition from a table.
     */
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

}
