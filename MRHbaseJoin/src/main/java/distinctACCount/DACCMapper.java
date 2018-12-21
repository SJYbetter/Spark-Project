package distinctACCount;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DACCMapper extends Mapper<Object, Text, Text, NullWritable> {
    NullWritable nullWritable = NullWritable.get();

    @Override
    public void map(Object key, Text acText, Context context) throws IOException, InterruptedException {
        context.write(acText, nullWritable);
    }
}
