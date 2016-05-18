import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by jing on 5/13/16.
 */
public class PartitionByDate {

    final static Logger logger = Logger.getLogger(PartitionByDate.class);
    final static String url = "jdbc:hive2://10.73.50.24:21050/;auth=noSasl";

    public static void main(String[] args) throws Exception {

        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("sourceDB")
                .hasArg()
                .isRequired()
                .withDescription("source DB name")
                .create('s'));
        options.addOption(OptionBuilder.withArgName("sourceTable")
                .hasArg()
                .isRequired()
                .withDescription("source table name")
                .create('o'));
        options.addOption(OptionBuilder.withArgName("targetDB")
                .hasArg()
                .isRequired()
                .withDescription("target DB name")
                .create('t'));
        options.addOption(OptionBuilder.withArgName("targetTable")
                .hasArg()
                .isRequired()
                .withDescription("target table name")
                .create('b'));


        try {
            CommandLine line;
            line = new BasicParser().parse(options, args);
            String sourceDB = line.getOptionValue('s');
            String sourceTable = line.getOptionValue('o');
            String targetDB = line.getOptionValue('t');
            String targetTable = line.getOptionValue('b');

            final String partSQL = "select distinct(substr(updated_at, 1, 10)) as part  from "
                    + sourceDB + "." + sourceTable + "order by part desc limit 1";
            Connection db = HiveDB.connectDB(url);
            String schema = ImpalaDB.getSchema(sourceDB, sourceTable, db);
            ImpalaDB.createPartitionTable(targetDB, targetTable, db, schema, "part_date int", "parquet");
            ParsePart parseMethod = new ParsePart() {
                public void parse(Partition part, String input) {
                    Partition.PartitionField<Integer> partDate = new Partition().new PartitionField<Integer>();
                    partDate.fieldName = "part_date";
                    partDate.fieldValue = Integer.parseInt(input.replaceAll("-", ""));
                    partDate.fieldString = input;
                    part.partFields.add(partDate);
                    return;

                }

                public String toString(Partition part) {
                    String result = "";
                    for (Partition.PartitionField field : part.partFields) {
                        result = field.fieldString;
                    }
                    return result;
                }
            };
            List<Partition> parts = ImpalaDB.getPartitions(db,
                    partSQL,
                    parseMethod);
            LockFile lock = new LockFile("/user/jing/lock", targetDB, targetTable);
            if (!lock.acquireLock()) {
                throw new IOException("can't acquire lock file" + lock.toString());
            }
            ImpalaDB.updatePartitions(db, parts,
                    sourceDB, sourceTable,
                    targetDB, targetTable,
                    parseMethod);

            if (!lock.releaseLock()) {
                throw new IOException("can't release lock file" + lock.toString());
            }

        } catch (SQLException e) {
            e.printStackTrace();
            logger.fatal("A sql exception is thrown, aborting the process.. " + e.getMessage());
            System.exit(1);

        } catch (IOException e) {
            e.printStackTrace();
            logger.fatal(e.getMessage());
            System.exit(1);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
