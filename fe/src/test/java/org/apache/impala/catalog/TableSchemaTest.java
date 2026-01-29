package org.apache.impala.catalog;

import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TVirtualColumnType;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableSchemaTest {

    @Test
    public void testEmptyTableSchema() throws ImpalaRuntimeException {
        TTable tTable = new TTable();
        tTable.setColumns(Collections.emptyList());
        tTable.setVirtual_columns(Collections.emptyList());
        tTable.setClustering_columns(Collections.emptyList());

        TableSchema schema = new TableSchema(tTable);

        assertTrue(schema.getColumns().isEmpty());
        assertArrayEquals(new String[]{}, schema.getColumnNames().toArray());
        assertTrue(schema.getVirtualColumns().isEmpty());
        assertTrue(schema.getClusteringColumns().isEmpty());
        assertTrue(schema.getNonClusteringColumns().isEmpty());
        assertEquals(0, schema.getNumClusteringCols());
        assertEquals(new ArrayType(new StructType()), schema.getType());
    }

    @Test
    public void testSimpleTableSchema() throws ImpalaRuntimeException {
        TColumn idColumn = new TColumn("id", Type.INT.toThrift());
        idColumn.setPosition(0);
        TColumn nameColumn = new TColumn("name", Type.STRING.toThrift());
        nameColumn.setPosition(1);

        TTable tTable = new TTable();
        tTable.setColumns(asList(idColumn, nameColumn));
        tTable.setVirtual_columns(Collections.emptyList());
        tTable.setClustering_columns(Collections.emptyList());

        TableSchema schema = new TableSchema(tTable);

        assertColumns(
            asList(
                new Column("id", Type.INT, 0),
                new Column("name", Type.STRING, 1)),
            schema.getColumns());
        assertArrayEquals(new String[]{"id", "name"}, schema.getColumnNames().toArray());
        assertTrue(schema.getVirtualColumns().isEmpty());
        assertTrue(schema.getClusteringColumns().isEmpty());
        assertEquals(0, schema.getNumClusteringCols());
        assertEquals(
            new ArrayType(new StructType(asList(
                new StructField("id", ScalarType.INT),
                new StructField("name", ScalarType.STRING)))),
            schema.getType());
    }

    @Test
    public void testSchemaWithVirtualColumns() throws ImpalaRuntimeException {
        TColumn idColumn = new TColumn("id", Type.INT.toThrift());
        idColumn.setPosition(0);
        TColumn virtualColumn = new TColumn(VirtualColumn.INPUT_FILE_NAME.getName(), Type.STRING.toThrift());
        virtualColumn.setVirtual_column_type(TVirtualColumnType.INPUT_FILE_NAME);
        virtualColumn.setPosition(0);

        TTable tTable = new TTable();
        tTable.setColumns(Collections.singletonList(idColumn));
        tTable.setVirtual_columns(Collections.singletonList(virtualColumn));
        tTable.setClustering_columns(Collections.emptyList());

        TableSchema schema = new TableSchema(tTable);

        assertColumns(
            Collections.singletonList(
                new Column("id", Type.INT, 0)),
                schema.getColumns());
        assertArrayEquals(new String[]{"id"}, schema.getColumnNames().toArray());
        assertColumns(
            Collections.singletonList(VirtualColumn.INPUT_FILE_NAME),
                schema.getVirtualColumns());
        assertTrue(schema.getClusteringColumns().isEmpty());
        assertEquals(0, schema.getNumClusteringCols());
        assertEquals(
            new ArrayType(new StructType(Collections.singletonList(
                new StructField("id", ScalarType.INT)))),
            schema.getType());
    }

    @Test
    public void testSchemaWithClusteringColumns() throws ImpalaRuntimeException {
        TColumn idColumn = new TColumn("id", Type.INT.toThrift());
        idColumn.setPosition(0);
        TColumn nameColumn = new TColumn("name", Type.STRING.toThrift());
        nameColumn.setPosition(1);

        TTable tTable = new TTable();
        tTable.setColumns(Collections.singletonList(idColumn));
        tTable.setVirtual_columns(Collections.emptyList());
        tTable.setClustering_columns(Collections.singletonList(nameColumn));

        TableSchema schema = new TableSchema(tTable);

        assertColumns(
            asList(
                new Column("name", Type.STRING, 1),
                new Column("id", Type.INT, 0)),
            schema.getColumns());
        assertArrayEquals(new String[]{"name", "id"}, schema.getColumnNames().toArray());
        assertEquals(0, schema.getVirtualColumns().size());
        assertColumns(
            Collections.singletonList(
                new Column("name", Type.STRING, 1)),
            schema.getClusteringColumns());
        assertEquals(1, schema.getNumClusteringCols());
        assertEquals(
                new ArrayType(new StructType(asList(
                        new StructField("name", ScalarType.STRING),
                        new StructField("id", ScalarType.INT)))),
                schema.getType());
    }

    private void assertColumns(List<? extends Column> expected, List<? extends Column> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            Column expectedColumn = expected.get(i);
            Column actualColumn = actual.get(i);
            assertEquals(expectedColumn.getName(), actualColumn.getName());
            assertEquals(expectedColumn.getPosition(), actualColumn.getPosition());
            assertEquals(expectedColumn.getComment(), actualColumn.getComment());
            assertEquals(expectedColumn.getType(), actualColumn.getType());
            if (expectedColumn instanceof VirtualColumn) {
                VirtualColumn expectedVColumn = (VirtualColumn) expectedColumn;
                VirtualColumn actualVColumn = (VirtualColumn) actualColumn;
                assertEquals(expectedVColumn.getVirtualColumnType(), actualVColumn.getVirtualColumnType());
            }
        }
    }
}