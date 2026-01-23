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
import java.util.Set;

import com.google.common.collect.Lists;

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.TableName;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;

/**
 * Helper class for creating CTAS target tables that can be used with Db and LocalDb
 * as well.
 */
public abstract class CtasTargetTable implements FeTable {
  protected final Table msTable_;
  protected final FeDb db_;
  protected final String name_;
  protected final String owner_;

  protected TableSchema schema_;

  public CtasTargetTable(org.apache.hadoop.hive.metastore.api.Table msTable, FeDb db,
  String name, String owner) {
    msTable_ = msTable;
    db_ = db;
    name_ = name;
    owner_ = owner;
  }

  @Override
  public boolean isLoaded() {
    return false;
  }

  @Override
  public Table getMetaStoreTable() {
    return msTable_;
  }

  @Override
  public String getStorageHandlerClassName() {
    return null;
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }

  @Override
  public FeDb getDb() { return db_; }

  @Override
  public String getName() { return name_; }

  @Override
  public String getFullName() { return (db_ != null ? db_.getName() + "." : "") + name_; }

  @Override
  public TableName getTableName() {
    return new TableName(db_ != null ? db_.getName() : null, name_);
  }

  @Override
  public List<Column> getColumnsInHiveOrder() {
    List<Column> columns = Lists.newArrayList(getNonClusteringColumns());
    columns = filterColumnsNotStoredInHms(columns);
    columns.addAll(getClusteringColumns());
    return Collections.unmodifiableList(columns);
  }

  @Override
  public List<Column> getClusteringColumns() {
    return schema_.getClusteringColumns();
  }

  @Override
  public List<Column> getNonClusteringColumns() {
    return schema_.getNonClusteringColumns();
  }

  @Override
  public int getNumClusteringCols() {
    return schema_.getNumClusteringCols();
  }

  @Override
  public boolean isClusteringColumn(Column c) {
    return schema_.isClusteringColumn(c);
  }

  @Override // FeTable
  public Column getColumn(String name) { return schema_.getColumn(name); }

  @Override
  public ArrayType getType() {
    return schema_.getType();
  }

  @Override
  public TableSchema getSchema() {
    return schema_;
  }

  @Override
  public long getNumRows() { return 0; }

  @Override
  public TTableStats getTTableStats() { return null; }

  @Override
  public abstract TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions);

  @Override
  public long getWriteId() { return 0; }

  @Override
  public ValidWriteIdList getValidWriteIds() { return null; }

  @Override
  public String getOwnerUser() {
    return owner_;
  }

  @Override
  public long getCatalogVersion() { return 0; }

  @Override
  public long getLastLoadedTimeMs() { return 0; }
}
