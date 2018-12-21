package custom;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ACPartitioner extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text acText, NullWritable nullWritable, int numPartitions) {
        Text aText = new Text(acText.toString().split(",")[0]);
        return aText.hashCode() % numPartitions;
    }
}
