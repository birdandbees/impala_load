import java.sql.*;
import java.util.List;

/**
 * Created by jing on 5/13/16.
 */
public class LoadMain {

	public static void main(String[] args) throws Exception {
		try {
			String url = "jdbc:hive2://10.73.50.24:21050/;auth=noSasl";
			Connection db = HiveDB.connectDB(url);
			String schema = ImpalaDB.getSchema("postgres", "postgres_avant_basic_us_payment_transactions_1", db);
			ImpalaDB.createPartitionTable("postgres_refined", "avant_basic_jtest", db, schema, "part_date int", "parquet");
			List<Partition> parts = ImpalaDB.getPartitions(db, "select distinct(substr(updated_at, 1, 10)) as part  from postgres.postgres_avant_basic_us_payment_transactions_1 order by part desc limit 1", new ParsePart() {
                public void parse(List<Partition.PartitionField> part, String input) {
                    Partition.PartitionField<Integer> partDate = new Partition().new PartitionField<Integer>();
                    partDate.fieldName = "part_date";
                    partDate.fieldValue = Integer.parseInt(input.replaceAll("-", ""));
                    partDate.fieldString = input;
                    part.add(partDate);
                    return;
                }
                public String toString(Partition p)
                {
                    return "";
                }

            });
			int ret = ImpalaDB.updatePartitions(db, parts, "postgres", "postgres_avant_basic_us_payment_transactions_1", "postgres_refined", "avant_basic_jtest", new ParsePart() {
                public void parse(List<Partition.PartitionField> parts, String input) {
                    return;
                }

                public String toString(Partition p) {
                    String result = "";
                    for (Partition.PartitionField field: p.partFields)
                    {
                        //result += field.fieldValue.toString() + "-";
                        result = field.fieldString;
                    }
                    //TODO: remove last '-'
                    //return result.substring(0, result.length()-1);
                    return result;

                }
            });

            System.out.println(ret);
		}
		catch (SQLException e)
		{
            e.printStackTrace();
            System.exit(1);

		}

	}

}

