package abc;

import common.common;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class BAMapper extends TableMapper<Text, Text> {
    Logger logger = LogManager.getLogger(BAMapper.class);
    byte[] cf = common.cf;
    byte[] cRfs = common.cRfs;

    Text bText = new Text();
    Text aText = new Text();

    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        String a = new String(row.get());
        logger.info("Get key: " + a);
        String references_str = new String(value.getValue(cf, cRfs));
        String[] references = references_str.split(",");

        aText.set(a);
        Counter counter = context.getCounter("Result", "Total");
        for (String b : references) {
            if(b.length() == 0)
                continue;
            counter.increment(1);
            bText.set(b);
            context.write(bText, aText);
        }
    }
}