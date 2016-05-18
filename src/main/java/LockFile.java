import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by jing on 5/17/16.
 */
public class LockFile {
    private Path lock;
    private FileSystem fs;

    public LockFile(String dir, String db, String table) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.73.48.255:7222/");
        fs = FileSystem.get(conf);
        lock = new Path(dir + "/" + db + "/" + table);
    }

    public boolean acquireLock() throws IOException {
        if (fs.exists(lock)) {
            return false;
        } else {
            fs.create(lock);
            return true;
        }

    }

    public boolean releaseLock() throws IOException {

        return fs.delete(lock, false);
    }

    public String toString() {
        return lock.toString();
    }

}
