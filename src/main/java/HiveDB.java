import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by jing on 4/4/16.
 */
public class HiveDB {

    public static Connection connectDB(String url) throws ClassNotFoundException, java.sql.SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection db = DriverManager.getConnection(url);
        return db;
    }

    public static ResultSet execQuery(Connection db, String sql) throws java.sql.SQLException {
        Statement st = db.createStatement();
        ResultSet rs = st.executeQuery(sql);
        return rs;
    }

    public static int execUpdate(Connection db, String sql) throws java.sql.SQLException {
        Statement st = db.createStatement();
        //st.close();
        return st.executeUpdate(sql);
    }

    public static void closeResultSet(ResultSet result) throws java.sql.SQLException {
        result.close();
    }


    public static void main(String[] args) {

    }

}
