package com.ouc.yyi.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: yyi
 * @date: 2018/7/16 16:22
 * @parameters:
 * @desc:使用mapreduce开发wordCount,设置删除路径
 **/
public class WordCountApp2 {
    /**
     *
     *读取输入的文件
     * 下面的Mapper 中的四个参数相当于两个key-value对，第一个是LongWritable是指文件的偏移量（Key），
     * 对应输入文本的偏移量，value是文本信息
     * 后面的是输出类型，输出的key是指的文本信息的单词的个数，value是每个单词出现的次数
     *
     */
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        LongWritable one = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           //1 将text类型转换成String的类型,接收到每一行数据
            String line = value.toString();
            //按照指定的字符进行分割
            String[] words = line.split(" ");
            for (String word:words){
                //根据指定的字符串分割后赋值1，通过上下文的输出将map的结果输出
                context.write(new Text(word),one);
            }

        }
    }
    /**
     * 归并操作
     * 重新reduce
     * */
    public static class MyReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum=0;
            for(LongWritable value:values){
                //求key出现的次数总和
                sum += value.get();
            }
            //最终统计结果的输出
            context.write(key,new LongWritable(sum));
        }
    }
    /**
     * 定义Driver 就是main
     * 封装了mapreduce作业的所有的信息
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建configuration
        Configuration conf = new Configuration();
        //准备清理已经存在的输出目录
        Path outPutPath= new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outPutPath)){
            fileSystem.delete(outPutPath,true);
            System.out.println("outPut file is exits!But it has deleted!");
        }
        //创建job作业
        Job job = Job.getInstance(conf,"wordcount");
        //设置job的处理类
        job.setJarByClass(WordCountApp2.class);
        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //设置map相关的参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置reduce相关的操作
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置作业的输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交作业
        job.waitForCompletion(true);
        System.out.println(job.waitForCompletion(true)?0:1);
    }
}
