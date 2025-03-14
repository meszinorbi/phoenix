package org.apache.phoenix.end2end.index;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class ATmpIndexIT extends BaseLocalIndexIT {
    public ATmpIndexIT(boolean isNamespaceMapped) {
        super(isNamespaceMapped);
    }

    @Test
    public void testIndexColumnOnUndefinedSchema() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String indexTableName = schemaName + "." + indexName;
        Connection conn1 = getConnection();
        try {

            createBaseTable(tableName, null, null);
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(V1)");
            conn1.commit();
            ResultSet rs = conn1.createStatement().executeQuery("SELECT * FROM " + indexTableName + " WHERE \":V1\" = 'a'");
            assertTrue(rs.next());
            fail();
        } catch (SQLException e) { // Expected
            assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(),e.getErrorCode());
        } finally {
            conn1.close();
        }
    }
}
