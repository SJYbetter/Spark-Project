package distinctACCount;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DACCCombiner extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    public void reduce(Text acText, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String nowA = "";
        String nowC = "";

        for(NullWritable value: values) {
            String[] a_c = acText.toString().split(",");
            String a = a_c[0];
            String c = a_c[1];

            if (a.equals(nowA) && c.equals(nowC))
                continue;

            context.write(acText, value);

            nowA = a;
            nowC = c;
        }
    }
}
