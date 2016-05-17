import java.util.ArrayList;
import java.util.List;

/**
 * Created by jing on 5/16/16.
 */
public class Partition {
    public class PartitionField<type> {
        type fieldValue;
        String fieldName;
        String fieldString;
        String serialize() { return fieldName + "="  + fieldValue.toString(); }

    }

    public List<PartitionField> partFields;
    public Partition(){};
    public void fill(ParsePart parseMethod, String input)
    {
        partFields = new ArrayList<PartitionField>();
        parseMethod.parse((List)partFields, input);
    }
    public String serialize()
    {
        String result="";
        for(PartitionField field: partFields) {
            result += field.serialize() + ",";
        }
        // TODO: remove last ','
        return result.substring(0, result.length()-1);
    }

    public String serializeValue(ParsePart parseMethod)
    {
        return parseMethod.toString(this);
    }
}
