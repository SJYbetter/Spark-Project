package distinctACCount;

import common.common;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class DACCReducer extends Reducer<Text, NullWritable, NullWritable, NullWritable> {
    String hTableName = common.hTableName;
    byte[] cf = common.cf;
    byte[] cRfs = common.cRfs;
    Table hTable = null;

    NullWritable nullWritable = NullWritable.get();

    String nowA = "";
    String nowC = "";
    HashSet<String> cSet = null;

    Counter counter = null;

    @Override
    public void setup(Context context) throws IOException {
        // Initialize hbase connection
        Configuration conf =  HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        hTable = connection.getTable(TableName.valueOf(hTableName));

        // Get counter
        counter = context.getCounter("Result", "Count");
    }

    @Override
    public void reduce(Text acText, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        nowA = "";
        nowC = "";
        cSet = new HashSet<>();

        for(NullWritable value: values) {
            String[] a_c = acText.toString().split(",");
            String a = a_c[0];
            String c = a_c[1];

            if (a.equals(nowA) && c.equals(nowC))
                continue;

            // if new a, update cSet
            if (!a.equals(nowA)) {
                cSet.clear();
                Get get = new Get(a.getBytes());
                get.addColumn(cf, cRfs);
                Result result = hTable.get(get);

                if(!result.isEmpty()) {
                    String[] cs = new String(result.getValue(cf, cRfs)).split(",");
                    for(String reference: cs) {
                        if(reference.length() == 0)
                            continue;
                        cSet.add(reference);
                    }
                }
            }

            if (cSet.contains(c)) {
                counter.increment(1);
            }

            nowA = a;
            nowC = c;
        }
    }
}
