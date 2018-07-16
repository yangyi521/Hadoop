package com.ouc.yyi.hadoop.hdfs;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * @author: yyi
 * @date: 2018/7/13 9:46
 * @parameters:
 * @desc:hadoop hdfs javaApi测试
 **/
public class HDFSApp {
    public static final String HDFS_PATH="hdfs://192.168.1.130:8020";
    FileSystem fileSystem = null;
    Configuration configuration = null;
    @Before
     public void setUp() throws Exception{
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"hadoop");//第三个参数为用户信息

    }
    /**
     * 创建hdfs目录
     * */
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test1"));

    }
    /**
     * 创建文件
     * */
    @Test
    public void create() throws Exception{
        FSDataOutputStream fsDataoutputStream = fileSystem.create(new Path("/hdfsapi/test1/a.txt"));
        fsDataoutputStream.write("hello hadoop".getBytes());
        fsDataoutputStream.close();

    }
    /**
     * 查看hdfs上的文件的内容
     * */
    @Test
    public void cat() throws Exception{
        FSDataInputStream fsDataInputStream=fileSystem.open(new Path("/hdfsapi/test1/a.txt"));
        IOUtils.copyBytes(fsDataInputStream,System.out,1024);
        fsDataInputStream.close();

    }
    /**
     * 重新命名
     * */
    @Test
    public void rename() throws Exception{
        Path oldPath = new Path("/hello");
        Path newPath = new Path("/newhello");
        fileSystem.rename(oldPath,newPath);

    }
    /**
     * 上传文件到hdfs
     * */
    @Test
    public void copyFromLocal() throws Exception{
        Path localPath = new Path("D:\\test.txt");
        Path hdfsPath = new Path("/test.txt");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);

    }
    /**
     * 上传文件到hdfs
     * */
    @Test
    public void copyFromLocalwithBar() throws Exception{
        Path localPath = new Path("D:\\daochu.dmp");
        Path hdfsPath = new Path("/daochu.dmp");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
        InputStream in = new BufferedInputStream(new FileInputStream(new File("D:\\daochu.dmp")));
        FSDataOutputStream output = fileSystem.create(new Path("/daochu.dmp"), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");//进度提醒
            }
        });
        IOUtils.copyBytes(in,output,4096);

    }
    /**
     * 下载文件到本地
     * */
    @Test
    public void copyToLocal() throws Exception{
        Path localPath=new Path("D:\\xiaoming.txt");
        Path hdfsPath=new Path("/newhello");
       fileSystem.copyToLocalFile(hdfsPath,localPath);
    }
    /**
     * 下载文件到本地
     * */
    @Test
    public void listFiles() throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for(FileStatus fileStatus:fileStatuses){
            String isDir = fileStatus.isDirectory()?"文件夹":"文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path );
        }
    }
    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path("/hdfsapi/test1/a.txt"),true);//是否递归删除

    }
    @After
     public void tearDown(){
         fileSystem = null;
         configuration = null;
     }
}
