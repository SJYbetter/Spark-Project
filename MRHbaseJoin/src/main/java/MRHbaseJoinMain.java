import abc.ABCReducer;
import abc.BAMapper;
import common.common;
import distinctACCount.DACCCombiner;
import distinctACCount.DACCMapper;
import distinctACCount.DACCReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MRHbaseJoinMain extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(MRHbaseJoinMain.class);
    Arguments arguments = new Arguments();

    @Override
    public int run(final String[] args) throws Exception {
        arguments.set(args);
        // Jobs
        Job abcJob = getABCJob();
        if (!abcJob.waitForCompletion(true)) return 1;

        Job distinctACJob = DACCJob();
        if (!distinctACJob.waitForCompletion(true)) return 1;

        long total = abcJob.getCounters().findCounter("Result", "Total").getValue();
        long count = distinctACJob.getCounters().findCounter("Result", "Count").getValue();

        logger.info("Total: " + total);
        logger.info("Count: " + count);

        return 0;
    }

    public static void main(final String[] args) {
        try {
            ToolRunner.run(new MRHbaseJoinMain(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

    // Distinct (a,c) + count
    Job DACCJob() throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "MR Join Hbaase Distinct");
        job.setJarByClass(MRHbaseJoinMain.class);

        job.setMapperClass(DACCMapper.class);
        job.setCombinerClass(DACCCombiner.class);
        job.setReducerClass(DACCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(custom.ACPartitioner.class);
        job.setGroupingComparatorClass(custom.ACGroupComparator.class);

        FileInputFormat.addInputPath(job, new Path(arguments.outputDirABC));
        FileOutputFormat.setOutputPath(job, new Path(arguments.outputDirDistinctAC));

        return job;
    }

    Job getABCJob() throws Exception {
        // hbase conf
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "MR Join Hbase ABC");
        job.setJarByClass(MRHbaseJoinMain.class);

        // Set hbase as input
        // Scan
        Scan scan = new Scan();
        scan.setCaching(arguments.hBaseCacheSize);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(
                common.hTableName,
                scan,
                BAMapper.class,
                Text.class,
                Text.class,
                job
        );

        job.setReducerClass(ABCReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(arguments.outputDirABC));

        return job;
    }

    private class Arguments {
        String outputDirABC = "";
        String outputDirDistinctAC = "";
        int hBaseCacheSize = 1;

        Arguments() {
        }

        void set(String[] args) {
            outputDirABC = args[0];
            outputDirDistinctAC = args[1];
            hBaseCacheSize = Integer.parseInt(args[2]);
        }
    }
}
