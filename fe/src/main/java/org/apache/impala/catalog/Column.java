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
import java.util.List;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.catalog.paimon.PaimonColumn;
import org.apache.impala.catalog.paimon.PaimonStructField;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnDescriptor;
import org.apache.impala.thrift.TColumnStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Internal representation of column-related metadata.
 * Owned by Catalog instance.
 */
public class Column {
  private final static Logger LOG = LoggerFactory.getLogger(Column.class);

  protected final String name_;
  protected final Type type_;
  protected final String comment_;
  protected int position_;  // in table

  protected final ColumnStats stats_;

  public Column(String name, Type type, int position) {
    this(name, type, null, position);
  }

  public Column(String name, Type type, String comment, int position) {
    Preconditions.checkState(name.equals(name.toLowerCase()));
    name_ = name;
    type_ = type;
    comment_ = comment;
    position_ = position;
    stats_ = new ColumnStats(type);
  }

  public String getComment() { return comment_; }
  public String getName() { return name_; }
  public Type getType() { return type_; }
  public int getPosition() { return position_; }
  public void setPosition(int position) { this.position_ = position; }
  public ColumnStats getStats() { return stats_; }
  public boolean isVirtual() { return false; }

  public boolean updateStats(ColumnStatisticsData statsData) {
    boolean statsDataCompatibleWithColType = stats_.update(name_, type_, statsData);
    if (LOG.isTraceEnabled()) {
      LOG.trace("col stats: " + name_ + " #distinct=" + stats_.getNumDistinctValues());
    }
    return statsDataCompatibleWithColType;
  }

  public void updateStats(TColumnStats statsData) {
    stats_.update(type_, statsData);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass())
                  .add("name_", name_)
                  .add("type_", type_)
                  .add("comment_", comment_)
                  .add("stats", stats_)
                  .add("position_", position_).toString();
  }

  public TColumn toThrift() {
    TColumn colDesc = new TColumn(name_, type_.toThrift());
    if (comment_ != null) colDesc.setComment(comment_);
    colDesc.setPosition(position_);
    colDesc.setCol_stats(getStats().toThrift());
    return colDesc;
  }

  public TColumnDescriptor toDescriptor() {
    return new TColumnDescriptor(getName(), getType().toThrift());
  }

  public static List<FieldSchema> toFieldSchemas(List<Column> columns) {
    return Lists.transform(columns, new Function<Column, FieldSchema>() {
      @Override
      public FieldSchema apply(Column column) {
        Preconditions.checkNotNull(column.getType());
        return new FieldSchema(column.getName(), column.getType().toSql().toLowerCase(),
            column.getComment());
      }
    });
  }

  public static List<String> toColumnNames(List<Column> columns) {
    List<String> colNames = new ArrayList<>();
    for (Column col: columns) colNames.add(col.getName());
    return colNames;
  }
  /**
   * Returns a struct type from the table columns passed in as a parameter.
   */
  public static StructType columnsToStruct(List<Column> columns) {
    List<StructField> fields = Lists.newArrayListWithCapacity(columns.size());
    for (Column col: columns) {
      if (col instanceof IcebergColumn) {
        // Create 'IcebergStructField' for Iceberg tables.
        IcebergColumn iCol = (IcebergColumn) col;
        fields.add(new IcebergStructField(iCol.getName(), iCol.getType(),
            iCol.getComment(), iCol.getFieldId()));
      } else if (col instanceof PaimonColumn) {
        PaimonColumn pCol = (PaimonColumn) col;
        fields.add(new PaimonStructField(pCol.getName(), pCol.getType(),
            pCol.getComment(), pCol.getFieldId(), pCol.isNullable()));
      } else {
        fields.add(new StructField(col.getName(), col.getType(), col.getComment()));
      }
    }
    return new StructType(fields);
  }

}
