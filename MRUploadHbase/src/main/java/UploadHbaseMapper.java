import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonValue;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UploadHbaseMapper extends Mapper<Object, Text, Object, Object> {
    // HBase Connection
    Configuration conf = null;
    HTable hTable = null;
    // Variables for json type input
    JsonParser jsonParser = null;
    Boolean useJson = false;
    // Variables for block type input
    String indexStart = "#index";
    String referenceStart = "#%";

    @Override
    public void setup(Context context) throws IOException {
        // Initialize HBase connection
        conf = HBaseConfiguration.create();
        hTable = new HTable(conf, common.hTableName);

        // Check input type
        useJson = context.getConfiguration().getBoolean("useJson", false);
        if (useJson)
            jsonParser = new JsonParser();
    }

    @Override
    public void map(Object key, Text blockText, Context context) throws IOException {
        String block = blockText.toString();
        String id = "";
        List<String> references = new ArrayList<>();

        if (useJson) {
            com.eclipsesource.json.JsonObject jsonObject = Json.parse(block).asObject();
            id = jsonObject.get("id").asString();

            com.eclipsesource.json.JsonValue referencesValue = jsonObject.get("references");
            if (referencesValue != null) {
                com.eclipsesource.json.JsonArray referencesArray = referencesValue.asArray();
                for (JsonValue reference : referencesArray)
                    references.add(reference.asString());
            }
        } else {
            for (String line : block.split("\n")) {
                if (line.startsWith(indexStart))
                    id = line.substring(indexStart.length());
                else if (line.startsWith(referenceStart))
                    references.add(line.substring(referenceStart.length()));
            }
        }


        // Put id, references to hbase
        if (id.length() == 0)
            return;
        Put put = new Put(id.getBytes());
        put.add(common.cf, common.cRfs, String.join(",", references).getBytes());
        hTable.put(put);
    }

    @Override
    public void cleanup(Context context) throws IOException {
        hTable.close();
    }
}
