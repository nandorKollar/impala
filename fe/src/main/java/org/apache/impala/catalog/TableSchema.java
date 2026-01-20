package org.apache.impala.catalog;

import com.google.common.base.Preconditions;
import org.apache.impala.catalog.paimon.PaimonColumn;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnDescriptor;
import org.apache.impala.thrift.TTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class represents the schema of a frontend table:
 * the list of columns and their data types.
 */
public final class TableSchema {
    // colsByPos[i] refers to the ith column in the table. The first numClusteringCols are
    // the clustering columns.
    private final List<Column> colsByPos_ = new ArrayList<>();

    // map from lowercase column name to Column object.
    private final Map<String, Column> colsByName_ = new HashMap<>();

    // Virtual columns of this table.
    private final List<VirtualColumn> virtualCols_ = new ArrayList<>();

    // Type of this table (array of struct) that mirrors the columns. Useful for analysis.
    private final ArrayType type_ = new ArrayType(new StructType());

    // Number of clustering columns.
    private final int numClusteringCols_;

    public TableSchema(TTable thriftTable) throws ImpalaRuntimeException {
        // TODO: we know the count of clustering and non-clustering columns,
        // hence we can allocate the corresponding lists with the right sizes
        List<TColumn> clusteringColumns = thriftTable.getClustering_columns();
        for (TColumn column : clusteringColumns) {
            addColumn(fromThrift(column));
        }
        for (TColumn column : thriftTable.getColumns()) {
            addColumn(fromThrift(column));
        }
        for (TColumn tvCol : thriftTable.getVirtual_columns()) {
            addVirtualColumn(VirtualColumn.fromThrift(tvCol));
        }
        this.numClusteringCols_ = clusteringColumns.size();
    }

    public List<Column> getColumns() { return colsByPos_; }

    public List<VirtualColumn> getVirtualColumns() { return virtualCols_; }

    public List<String> getColumnNames() { return Column.toColumnNames(colsByPos_); }

    public List<Column> getClusteringColumns() {
        return Collections.unmodifiableList(colsByPos_.subList(0, numClusteringCols_));
    }

    public ArrayType getType() { return type_; }

    public List<Column> getNonClusteringColumns() {
        return Collections.unmodifiableList(colsByPos_.subList(numClusteringCols_,
                colsByPos_.size()));
    }

    public Column getColumn(String name) { return colsByName_.get(name.toLowerCase()); }

    public boolean isClusteringColumn(Column c) {
        return c.getPosition() < numClusteringCols_;
    }

    public void addColumn(Column col) {
        colsByPos_.add(col);
        colsByName_.put(col.getName().toLowerCase(), col);
        ((StructType) type_.getItemType()).addField(
                new StructField(col.getName(), col.getType(), col.getComment()));
    }

    void clearColumns() {
        colsByPos_.clear();
        colsByName_.clear();
        ((StructType) type_.getItemType()).clearFields();
        virtualCols_.clear();
    }

    private void addVirtualColumn(VirtualColumn col) {
        virtualCols_.add(col);
    }

    public int getNumClusteringCols() { return numClusteringCols_; }

    /**
     * Sets the number of clustering columns. This method should only be used for tests and
     * the caller must make sure that the value matches any columns that were added to the
     * table.
     */
    public void setNumClusteringCols(int n) {
//        Preconditions.checkState(RuntimeEnv.INSTANCE.isTestEnv());
//        numClusteringCols_ = n;
    }

    /**
     * Checks preconditions for this table to function as expected. Currently only checks
     * that all entries in colsByName_ use lower case keys.
     */
    public void validate() throws TableLoadingException {
        for (String colName : colsByName_.keySet()) {
            if (!colName.equals(colName.toLowerCase())) {
                throw new TableLoadingException(
                        "Expected lower case column name but found: " + colName);
            }
        }
    }

    /**
     * Returns a list of thrift column descriptors ordered by position.
     */
    public List<TColumnDescriptor> toTColumnDescriptors() {
        List<TColumnDescriptor> colDescs = new ArrayList<>();
        for (Column col: colsByPos_) {
            colDescs.add(col.toDescriptor());
        }
        return colDescs;
    }

    private Column fromThrift(TColumn columnDesc) throws ImpalaRuntimeException {
        String comment = columnDesc.isSetComment() ? columnDesc.getComment() : null;
        Preconditions.checkState(columnDesc.isSetPosition());
        int position = columnDesc.getPosition();
        Type type = Type.fromThrift(columnDesc.getColumnType());
        Column col;
        if (columnDesc.isIs_iceberg_column()) {
            Preconditions.checkState(columnDesc.isSetIceberg_field_id());
            col = new IcebergColumn(columnDesc.getColumnName(), type, comment, position,
                    columnDesc.getIceberg_field_id(), columnDesc.getIceberg_field_map_key_id(),
                    columnDesc.getIceberg_field_map_value_id(), columnDesc.isIs_nullable());
        } else if (columnDesc.isIs_paimon_column()) {
            Preconditions.checkState(columnDesc.isSetIceberg_field_id());
            col = new PaimonColumn(columnDesc.getColumnName(), type, comment, position,
                    columnDesc.getIceberg_field_id(), columnDesc.isIs_nullable());
        } else if (columnDesc.isIs_hbase_column()) {
            // HBase table column. The HBase column qualifier (column name) is not be set for
            // the HBase row key, so it being set in the thrift struct is not a precondition.
            Preconditions.checkState(columnDesc.isSetColumn_family());
            Preconditions.checkState(columnDesc.isSetIs_binary());
            col = new HBaseColumn(columnDesc.getColumnName(), columnDesc.getColumn_family(),
                    columnDesc.getColumn_qualifier(), columnDesc.isIs_binary(),
                    type, comment, position);
        } else if (columnDesc.isIs_kudu_column()) {
            col = KuduColumn.fromThrift(columnDesc, position);
        } else {
            // Hdfs table column.
            col = new Column(columnDesc.getColumnName(), type, comment, position);
        }
        if (columnDesc.isSetCol_stats()) col.updateStats(columnDesc.getCol_stats());
        return col;
    }
}
