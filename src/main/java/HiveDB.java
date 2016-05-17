import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by jing on 4/4/16.
 */
public class HiveDB {
    final static Logger logger = Logger.getLogger(HiveDB.class);

    public static Connection connectDB(String url) throws ClassNotFoundException, java.sql.SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection db = DriverManager.getConnection(url);
        logger.debug("connected to " + url);
        return db;
    }

    public static ResultSet execQuery(Connection db, String sql) throws java.sql.SQLException {
        Statement st = db.createStatement();
        ResultSet rs = st.executeQuery(sql);
        logger.debug("run query: " + sql);
        return rs;
    }

    public static void execUpdate(Connection db, String sql) throws java.sql.SQLException {
        Statement st = db.createStatement();
        logger.debug("run update: " + sql);
        st.executeUpdate(sql);
        st.close();
        return;
    }

    public static void closeResultSet(ResultSet result) throws java.sql.SQLException {
        result.close();
    }

}
