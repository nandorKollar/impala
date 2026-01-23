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

package org.apache.impala.catalog.local;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HBaseTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.SideloadTableStats;
import org.apache.impala.catalog.SystemTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.TableSchema;
import org.apache.impala.catalog.VirtualColumn;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.catalog.paimon.PaimonUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.MetadataOp;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TTableStats;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table instance loaded from {@link LocalCatalog}.
 *
 * This class is not thread-safe. A new instance is created for
 * each catalog instance.
 */
abstract class LocalTable implements FeTable {
  private static final Logger LOG = LoggerFactory.getLogger(LocalTable.class);

  protected final LocalDb db_;
  /** The lower-case name of the table. */
  protected final String name_;
  protected final TImpalaTableType tableType_;
  protected final String tableComment_;

  private final TableSchema schema_;

  protected final Table msTable_;

  private final TTableStats tableStats_;

  // Test only stats that will be injected in place of stats obtained from HMS.
  protected SideloadTableStats testStats_ = null;

  // Scale factor to multiply table stats with. Only used for testing.
  protected double testMetadataScale_ = 1.0;

  /**
   * Table reference as provided by the initial call to the metadata provider.
   * This must be passed back to any further calls to the metadata provider
   * in order to verify consistency.
   *
   * In the case of CTAS target tables, this may be null. Since the tables don't
   * exist yet in any metadata storage, it would be invalid to try to load any metadata
   * about them.
   */
  @Nullable
  @VisibleForTesting
  protected final TableMetaRef ref_;

  public static LocalTable load(LocalDb db, String tblName) throws TableLoadingException {
    Pair<Table, TableMetaRef> tableMeta = loadTableMetadata(db, tblName);
    return load(db, tableMeta);
  }

  public static LocalTable load(LocalDb db, Pair<Table, TableMetaRef> tableMeta)
      throws TableLoadingException {
    // In order to know which kind of table subclass to instantiate, we need
    // to eagerly grab and parse the top-level Table object from the HMS.
    LocalTable t = null;
    Table msTbl = tableMeta.first;
    TableMetaRef ref = tableMeta.second;
    if (TableType.valueOf(msTbl.getTableType()) == TableType.VIRTUAL_VIEW) {
      t = new LocalView(db, msTbl, ref);
    } else if (HBaseTable.isHBaseTable(msTbl)) {
      t = LocalHbaseTable.loadFromHbase(db, msTbl, ref);
    } else if (KuduTable.isKuduTable(msTbl)) {
      t = LocalKuduTable.loadFromKudu(db, msTbl, ref);
    } else if (IcebergTable.isIcebergTable(msTbl)) {
      t = LocalIcebergTable.loadIcebergTableViaMetaProvider(db, msTbl, ref);
    } else if (PaimonUtil.isPaimonTable(msTbl)) {
      t = LocalPaimonTable.load(db, msTbl, ref);
    } else if (DataSourceTable.isDataSourceTable(msTbl)) {
      t = LocalDataSourceTable.load(db, msTbl, ref);
    } else if (SystemTable.isSystemTable(msTbl)) {
      t = LocalSystemTable.load(db, msTbl, ref);
    } else if (HdfsFileFormat.isHdfsInputFormatClass(msTbl.getSd().getInputFormat())) {
      t = LocalFsTable.load(db, msTbl, ref);
    }

    if (t == null) {
      throw new LocalCatalogException("Unknown table type for table " +
          db.getName() + "." + msTbl.getTableName());
    }

    // TODO(todd): it would be preferable to only load stats for those columns
    // referenced in a query, but there doesn't seem to be a convenient spot
    // in between slot reference resolution and where the stats are needed.
    // So, for now, we'll just load all the column stats up front.
    t.loadColumnStats();
    return t;
  }


  /**
   * Load the Table instance from the metastore.
   */
  private static Pair<Table, TableMetaRef> loadTableMetadata(LocalDb db, String tblName)
      throws TableLoadingException {
    Preconditions.checkArgument(tblName.toLowerCase().equals(tblName));

    try {
      return db.getCatalog().getMetaProvider().loadTable(db.getName(), tblName);
    } catch (TException e) {
      throw new TableLoadingException(String.format(
          "Could not load table %s.%s from catalog",
          db.getName(), tblName), e);
    }
  }

  public LocalTable(LocalDb db, Table msTbl, TableMetaRef ref, TableSchema schema) {
    this.db_ = Preconditions.checkNotNull(db);
    this.name_ = msTbl.getTableName();
    this.tableType_ = MetastoreShim.mapToInternalTableType(msTbl.getTableType());
    this.tableComment_ = MetadataOp.getTableComment(msTbl);
    this.schema_ = schema;
    this.ref_ = ref;
    this.msTable_ = msTbl;

    if (RuntimeEnv.INSTANCE.hasSideloadStats(db.getName(), name_)) {
      testStats_ = RuntimeEnv.INSTANCE.getSideloadStats(db.getName(), name_);
    }

    tableStats_ = new TTableStats(-1);
    long rowCount = FeCatalogUtils.getRowCount(msTable_.getParameters());
    if (testStats_ != null) {
      tableStats_.setTotal_file_bytes(testStats_.getTotalSize());
      testMetadataScale_ = (double) testStats_.getNumRows() / rowCount;
      rowCount = testStats_.getNumRows();
    } else {
      tableStats_.setTotal_file_bytes(
          FeCatalogUtils.getTotalSize(msTable_.getParameters()));
    }
    tableStats_.setNum_rows(rowCount);
  }

  public LocalTable(LocalDb db, Table msTbl, TableMetaRef ref) {
    this(db, msTbl, ref, TableSchema.fromMsTable(msTbl));
  }

  protected LocalTable(LocalDb db, String tblName) {
    this.db_ = Preconditions.checkNotNull(db);
    this.name_ = tblName;
    this.tableType_ = TImpalaTableType.TABLE;
    this.tableComment_ = null;
    this.ref_ = null;
    this.msTable_ = null;
    this.schema_ = null;
    this.tableStats_ = null;
  }

  protected void addVirtualColumns(List<VirtualColumn> virtualColumns) {
    for (VirtualColumn virtCol : virtualColumns) addVirtualColumn(virtCol);
  }

  protected void addVirtualColumn(VirtualColumn col) {
    schema_.addVirtualColumn(col);
  }

  @Override
  public boolean isLoaded() {
    return true;
  }

  @Override
  public Table getMetaStoreTable() {
    return msTable_;
  }

  @Override
  public String getOwnerUser() {
    if (msTable_ == null) {
      LOG.warn("Owner of {} is unknown due to msTable is unloaded", getFullName());
      return null;
    }
    return MetastoreShim.getTableOwnerType(msTable_) == PrincipalType.USER ?
        msTable_.getOwner() : null;
  }

  @Override
  public String getStorageHandlerClassName() {
    // Subclasses should override as appropriate.
    return null;
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }

  @Override
  public String getName() {
    return name_;
  }

  @Override
  public String getFullName() {
    return db_.getName() + "." + name_;
  }

  @Override
  public TableName getTableName() {
    return new TableName(db_.getName(), name_);
  }

  @Override
  public TImpalaTableType getTableType() {
    return tableType_;
  }

  @Override
  public String getTableComment() {
    return tableComment_;
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
  public List<VirtualColumn> getVirtualColumns() { return schema_.getVirtualColumns(); }

  @Override
  public int getNumClusteringCols() {
    return schema_.getNumClusteringCols();
  }

  @Override
  public boolean isClusteringColumn(Column c) {
    return schema_.isClusteringColumn(c);
  }

  @Override
  public FeDb getDb() {
    return db_;
  }

  @Override
  public long getNumRows() {
    return tableStats_.num_rows;
  }

  @Override
  public TTableStats getTTableStats() {
    return tableStats_;
  }

  @Override
  public long getWriteId() {
    return -1l;
  }

  @Override
  public ValidWriteIdList getValidWriteIds() {
    return null;
  }

  @Override
  public long getCatalogVersion() {
    if (ref_ == null) return 0;
    return ref_.getCatalogVersion();
  }

  @Override
  public long getLastLoadedTimeMs() {
    if (ref_ == null) return 0;
    return ref_.getLoadedTimeMs();
  }

  protected void loadColumnStats() {
    try {
      List<ColumnStatisticsObj> stats = db_.getCatalog().getMetaProvider()
          .loadTableColumnStatistics(ref_, schema_.getColumnNames());
      FeCatalogUtils.injectColumnStats(stats, this, testStats_);
    } catch (TException e) {
      LOG.warn("Could not load column statistics for: " + getFullName(), e);
    }
  }

  protected double getDebugMetadataScale() { return testMetadataScale_; }

  @Override
  public TableSchema getSchema() {
    return schema_;
  }
}
