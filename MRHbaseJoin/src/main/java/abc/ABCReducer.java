package abc;

import common.common;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ABCReducer extends Reducer<Text, Text, Text, NullWritable> {
    String hTableName = common.hTableName;
    byte[] cf = common.cf;
    byte[] cRfs = common.cRfs;
    Table hTable = null;

    Text acText = new Text();
    NullWritable nullValue = NullWritable.get();

    @Override
    public void setup(Context context) throws IOException{
        Configuration conf =  HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        hTable = connection.getTable(TableName.valueOf(hTableName));
    }

    @Override
    public void reduce(Text key, Iterable<Text> aIterator, Context context) throws IOException, InterruptedException {
        String b = key.toString();
        Get get = new Get(b.getBytes());
        get.addColumn(cf, cRfs);
        Result result = hTable.get(get);

        if (result.isEmpty())
            return;

        String[] cs = new String(result.getValue(cf, cRfs)).split(",");

        for(Text aText: aIterator) {
            for(String c: cs) {
                if(c.length() == 0)
                    continue;
                String ac = aText.toString() + "," + c;
                acText.set(ac);

                context.write(acText, nullValue);
            }
        }
    }
}