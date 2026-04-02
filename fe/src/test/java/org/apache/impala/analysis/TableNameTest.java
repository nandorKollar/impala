package org.apache.impala.analysis;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TableNameTest {
    @Test
    public void testFullName() {
        assertEquals("db.tbl",
            new TableName("db", "tbl").fullName());
        assertEquals("db.tbl.vTbl",
            new TableName("db", "tbl", "vTbl").fullName());
        assertEquals("tbl",
                new TableName(null, "tbl", "vTbl").fullName());
        assertEquals("tbl",
                new TableName(null, "tbl", null).fullName());
    }
}
