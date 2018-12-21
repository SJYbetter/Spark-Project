import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MRUploadHbaseMain extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(MRUploadHbaseMain.class);
    Arguments arguments = new Arguments();

    @Override
    public int run(final String[] args) throws Exception {
        arguments.set(args);
        // Initalize Hbase table
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);

        if(admin.isTableAvailable(common.hTableName)) {
            logger.info("droping original table " + common.hTableName);
            admin.disableTable(common.hTableName);
            admin.deleteTable(common.hTableName);
        }

        logger.info("creating table " + common.hTableName);
        HTableDescriptor tableDescriptor = new HTableDescriptor(common.hTableName);
        tableDescriptor.addFamily(new HColumnDescriptor(common.cf));
        admin.createTable(tableDescriptor);

        // Job
        Job job = getJob(args);
        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(final String[] args) {
        try{
            ToolRunner.run(new MRUploadHbaseMain(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

    Job getJob(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "MR Upload Hbase");
        job.setJarByClass(MRUploadHbaseMain.class);
        Configuration jobConf = job.getConfiguration();

        jobConf.setBoolean("useJson", arguments.useJson);

        String delim = arguments.useJson ? "\n" : "\n\n";
        jobConf.set("textinputformat.record.delimiter", delim);

        job.setMapperClass(UploadHbaseMapper.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(arguments.inputFile));
        FileOutputFormat.setOutputPath(job, new Path(arguments.outputFile));

        deleteFolder(new Path(arguments.outputFile), jobConf);

        return job;
    }

    private void deleteFolder(Path path, Configuration conf) throws Exception {
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(path)){
            fileSystem.delete(path, true);
        }
    }

    private class Arguments {
        String inputFile = "";
        String inputFileType = "";
        String outputFile = "";
        boolean useJson = false;

        Arguments() {
        }

        void set(String[] args) {
            inputFile = args[0];
            inputFileType = args[1];
            outputFile = args[2];

            useJson = inputFileType.equals("json");
        }
    }
}
