package hadoop.mapreduce.job;

import hadoop.mapreduce.mapper.WCMapper;
import hadoop.mapreduce.reducer.WCReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 描述一个特定的作业
 * 指定作业使用哪个map和哪个reduce
 * 指定作业要处理的数据的路径
 * 指定作业输出的结果的路径
 */
public class WCRunner {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        // 在linux环境的idea下运行，需将jar包导入，添加基本yarn配置(mapred-site.xml,yarn-site.xml)
        // 且需加入配置,conf.set("mapreduce.job.jar","xxx.jar");

        Job wcJob = Job.getInstance(conf);

        // 设置整个job所用的那些类在哪个jar包
        wcJob.setJarByClass(WCRunner.class);


        // job使用的mapper和reducer类
        wcJob.setMapperClass(WCMapper.class);
        wcJob.setReducerClass(WCReducer.class);

        // 指定reduce的输出数据kv类型
        wcJob.setOutputKeyClass(Text.class);
        wcJob.setOutputValueClass(LongWritable.class);

        // 指定mapper的输出数据kv类型
        wcJob.setMapOutputKeyClass(Text.class);
        wcJob.setMapOutputValueClass(LongWritable.class);

        // 指定输入数据存放路径
        FileInputFormat.setInputPaths(wcJob, new Path("/wc/data"));
        // 指定处理结果输出路径
        FileOutputFormat.setOutputPath(wcJob, new Path("/wc/output"));

        // 将job提交给集群运行,boolean值为true表示显示执行过程
        wcJob.waitForCompletion(true);
    }

}
