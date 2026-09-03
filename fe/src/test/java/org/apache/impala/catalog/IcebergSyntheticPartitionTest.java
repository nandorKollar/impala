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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.apache.impala.catalog.CatalogObject.ThriftObjectType;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.util.IcebergUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IcebergSyntheticPartitionTest {

  private CatalogServiceCatalog catalog_;

  @Before
  public void init() {
    catalog_ = CatalogServiceTestCatalog.create();
  }

  @After
  public void cleanUp() { catalog_.close(); }

  @Test
  public void testSyntheticPartitionStructure() throws Exception {
    IcebergTable iceTbl = loadIcebergTable(
        "functional_parquet", "iceberg_partitioned");

    THdfsTable synthetic = iceTbl.buildSyntheticTHdfsTable(ThriftObjectType.FULL);
    assertNotNull(synthetic);

    assertEquals(iceTbl.getFeFsTable().getHdfsBaseDir(), synthetic.getHdfsBaseDir());
    assertEquals(iceTbl.getColumnNames(), synthetic.getColNames());

    Map<Long, THdfsPartition> partitions = synthetic.getPartitions();
    assertEquals(1, partitions.size());
    assertTrue(partitions.containsKey(IcebergTable.ICEBERG_PARTITION_ID));

    THdfsPartition partition = partitions.get(IcebergTable.ICEBERG_PARTITION_ID);
    assertEquals(IcebergTable.ICEBERG_PARTITION_ID, partition.getId());
    assertTrue(partition.getPartitionKeyExprs().isEmpty());
    assertEquals(
        IcebergUtil.toTHdfsFileFormat(iceTbl.getIcebergFileFormat()),
        partition.getHdfs_storage_descriptor().getFileFormat());

    assertNotNull(synthetic.getPrototype_partition());
    assertNotNull(synthetic.getNetwork_addresses());
    assertTrue(synthetic.getNetwork_addresses().size() > 0);

    assertEquals(1, synthetic.getPartition_prefixes().size());
    assertEquals(iceTbl.getFeFsTable().getHdfsBaseDir(),
        synthetic.getPartition_prefixes().get(0));
  }

  @Test
  public void testSyntheticMatchesExistingOutput() throws Exception {
    IcebergTable iceTbl = loadIcebergTable(
        "functional_parquet", "iceberg_partitioned");

    THdfsTable existing = iceTbl.transformToTHdfsTable(true, ThriftObjectType.FULL);
    THdfsTable synthetic = iceTbl.buildSyntheticTHdfsTable(ThriftObjectType.FULL);

    assertEquals(existing.getHdfsBaseDir(), synthetic.getHdfsBaseDir());
    assertEquals(existing.getColNames(), synthetic.getColNames());
    assertEquals(existing.getNullPartitionKeyValue(),
        synthetic.getNullPartitionKeyValue());
    assertEquals(existing.getNullColumnValue(), synthetic.getNullColumnValue());

    assertEquals(1, synthetic.getPartitions().size());

    THdfsPartition existingPart = existing.getPartitions().values().iterator().next();
    THdfsPartition syntheticPart =
        synthetic.getPartitions().get(IcebergTable.ICEBERG_PARTITION_ID);

    assertEquals(
        existingPart.getHdfs_storage_descriptor().getFileFormat(),
        syntheticPart.getHdfs_storage_descriptor().getFileFormat());
  }

  @Test
  public void testSyntheticDescriptorOnly() throws Exception {
    IcebergTable iceTbl = loadIcebergTable(
        "functional_parquet", "iceberg_partitioned");

    THdfsTable synthetic =
        iceTbl.buildSyntheticTHdfsTable(ThriftObjectType.DESCRIPTOR_ONLY);
    assertNotNull(synthetic);
    assertNotNull(synthetic.getPartitions());
    assertEquals(1, synthetic.getPartitions().size());
    assertTrue(!synthetic.isSetNetwork_addresses()
        || synthetic.getNetwork_addresses().isEmpty());
  }

  @Test
  public void testSyntheticWithV2Table() throws Exception {
    IcebergTable iceTbl = loadIcebergTable(
        "functional_parquet",
        "iceberg_v2_positional_not_all_data_files_have_delete_files");

    THdfsTable synthetic = iceTbl.buildSyntheticTHdfsTable(ThriftObjectType.FULL);
    assertNotNull(synthetic);
    assertEquals(1, synthetic.getPartitions().size());
    assertTrue(synthetic.getPartitions()
        .containsKey(IcebergTable.ICEBERG_PARTITION_ID));
  }

  private IcebergTable loadIcebergTable(String dbName, String tblName) throws Exception {
    Table tbl = catalog_.getOrLoadTable(dbName, tblName, "test", null);
    assertTrue(tbl instanceof IcebergTable);
    return (IcebergTable) tbl;
  }
}
