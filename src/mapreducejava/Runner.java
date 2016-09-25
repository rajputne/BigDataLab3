package mapreducejava;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Runner {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Runner.class);
        conf.setJobName("ip-count");

        conf.setMapperClass(IpMapper.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        conf.setReducerClass(IpReducer.class);

        // take the input and output from the command line
        String path = "C:\\Users\\User\\Desktop\\Big Data\\Lecture2\\hadoopLabs\\Lab3.begin\\Logs";
        String outpath = "C:\\Users\\User\\Desktop\\Big Data\\Lecture2\\hadoopLabs\\Lab3.begin\\output";
        FileInputFormat.setInputPaths(conf, new Path(path));
        FileOutputFormat.setOutputPath(conf, new Path(outpath));
        JobClient.runJob(conf);
    }
}
