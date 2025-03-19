package org.apache.phoenix.end2end.index;

import org.apache.phoenix.exception.SQLExceptionCode;
//import org.apache.phoenix.util.bson.TestUtil;
import org.apache.phoenix.schema.PIndexState;
import org.junit.Test;
import org.apache.phoenix.util.TestUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class ATmpIndexIT extends BaseLocalIndexIT {
    public ATmpIndexIT(boolean isNamespaceMapped) {
        super(isNamespaceMapped);
    }
    /*
    The problem lies in the double quotes surrounding the value: f in the select query
     */
    @Test
    public void testVarcharSurroundedByDoubleQuotesOnLocalIndex() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String indexTableName = schemaName + "." + indexName;
        Connection conn1 = getConnection();
        try {
            if (isNamespaceMapped) {
                conn1.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            }
            String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" +
                    "k1 INTEGER NOT NULL,\n" +
                    "k2 INTEGER NOT NULL,\n" +
                    "k3 INTEGER,\n" +
                    "v1 VARCHAR,\n" +
                    "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n";
            conn1.createStatement().execute(ddl);
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(V1)");
            conn1.commit();
            assertEquals(PIndexState.ACTIVE, TestUtil.getIndexState(conn1, indexTableName));
            assertEquals(4, TestUtil.getRowCount(conn1, indexTableName));
            ResultSet rs = conn1.createStatement()
                    .executeQuery("SELECT * FROM " + indexTableName + " WHERE \":T_ID\" = \"f\"");
            assertTrue(rs.next());
            fail();
        } catch (SQLException e) { // Expected
            assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(),e.getErrorCode());
        } finally {
            conn1.close();
        }
    }
}
