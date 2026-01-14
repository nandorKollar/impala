package org.apache.impala.catalog;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

public class TableSchemaTest {

    @Test
    public void testSchema() {
        TableSchema schema = new TableSchema(
                asList(
                        new Column("col1", Type.INT, -1),
                        new Column("col2", Type.STRING, -1),
                        new Column("col3", Type.BIGINT, -1)),
                emptyList(),
                1);

        assertEquals(3, schema.getColumns().size());
        assertEquals(1, schema.getClusteringColumns().size());
        assertEquals(asList("col1", "col2", "col3"), schema.getColumnNames());
        assertEquals(1, schema.getNumClusteringCols());
    }
}