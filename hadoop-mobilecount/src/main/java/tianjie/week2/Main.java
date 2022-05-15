package tianjie.week2;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        args = new String[]{"hdfs://emr-header-1.cluster-285604:9000/user/student5/tianjie/input/HTTP_20130313143750.dat", "hdfs://emr-header-1.cluster-285604:9000/user/student5/tianjie/output"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Main.class);
        job.setMapperClass(MobileMap.class);
        job.setReducerClass(MobileReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MobileBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MobileBean.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileInputFormat.setInputPaths(job, new Path[]{inputPath});
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}