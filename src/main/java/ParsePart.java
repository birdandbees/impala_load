
import java.util.List;

/**
 * Created by jing on 5/17/16.
 */
public interface ParsePart {

    public void parse(List<Partition.PartitionField>parts, String input);
    public String toString(Partition p);
}
