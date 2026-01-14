package org.apache.impala.catalog;

import org.apache.impala.thrift.TColumnDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TableSchema {
    // colsByPos[i] refers to the ith column in the table. The first numClusteringCols are
    // the clustering columns.
    private final List<Column> colsByPos_ = new ArrayList<>();

    // map from lowercase column name to Column object.
    private final Map<String, Column> colsByName_ = new HashMap<>();

    // Virtual columns of this table.
    private final List<VirtualColumn> virtualCols_;

    // Type of this table (array of struct) that mirrors the columns. Useful for analysis.
    private final ArrayType type_ = new ArrayType(new StructType());

    // Number of clustering columns.
    private final int numClusteringCols_;

    public TableSchema(List<Column> columns, List<VirtualColumn> virtualColumns, int numClusteringCols) {
        for (Column c: columns) {
            addColumn(c);
        }
        this.virtualCols_ = virtualColumns;
        this.numClusteringCols_ = numClusteringCols;
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

    private void addColumn(Column col) {
        colsByPos_.add(col);
        colsByName_.put(col.getName().toLowerCase(), col);
        ((StructType) type_.getItemType()).addField(
                new StructField(col.getName(), col.getType(), col.getComment()));
    }

    public void clearColumns() {
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
}
