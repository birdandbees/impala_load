import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jing on 5/16/16.
 */
public class ImpalaDB {

    final static Logger logger = Logger.getLogger(ImpalaDB.class);

    public static String getSchema(String database, String table, Connection db) throws SQLException {
        ResultSet res = HiveDB.execQuery(db, "describe " + database + "." + table);
        StringBuilder result = new StringBuilder();
        while (res.next()) {
            result.append(res.getString(1) + "  " + res.getString(2) + ",");
        }
        res.close();
        logger.debug("schema is " + result.toString());
        return result.substring(0, result.length() - 1).toString();
    }

    public static void createPartitionTable(String targetDB, String targetTable, Connection db, String schema, String partitionClause, String tableFormat)
            throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists ");
        sql.append(targetDB);
        sql.append(".");
        sql.append(targetTable);
        sql.append(" (");
        sql.append(schema);
        sql.append(" ) partitioned by (");
        sql.append(partitionClause);
        sql.append(") stored as ");
        sql.append(tableFormat);
        HiveDB.execUpdate(db, sql.toString());
        return;


    }

    public static List<Partition> getPartitions(Connection db, String sql, ParsePart parseMethod) throws SQLException {
        ResultSet rs = HiveDB.execQuery(db, sql);
        List<Partition> parts = new ArrayList<Partition>();
        while (rs.next()) {
            Partition part = new Partition();
            part.fill(parseMethod, rs.getString(1));
            parts.add(part);
        }
        rs.close();
        return parts;
    }

    public static void updatePartitions(Connection db, List<Partition> parts,
                                        String sourceDB, String sourceTable,
                                        String targetDB, String targetTable, ParsePart parseMethod)
            throws SQLException {
        for (Partition part : parts) {
            StringBuilder sql = new StringBuilder();
            sql.append("insert into ");
            sql.append(targetDB);
            sql.append(".");
            sql.append(targetTable);
            sql.append(" partition(");
            sql.append(part.serialize());
            sql.append(") select * from ");
            sql.append(sourceDB);
            sql.append(".");
            sql.append(sourceTable);
            sql.append(" where substr(updated_at, 1, 10) =\'");
            sql.append(part.serializeValue(parseMethod));
            sql.append("\'");
            logger.debug(sql.toString());
            HiveDB.execUpdate(db, sql.toString());
        }
        return;
    }

    public static void mergePartitions() {

    }


}
