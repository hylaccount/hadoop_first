package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * Created by Administrator on 2017/3/21.
 */
public class HDFSOperate {
    FileSystem fs = null;
    @Before//初始化下面的方法
    public void getFs(){
        //获取一个filesystem
        try {
            fs=  FileSystem.get(
                    new URI("hdfs://ibeifeng.com:8020"),
                    new Configuration(),"user01");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testDown(){
        //下载HDFS文件
        try {
            FSDataInputStream fin=fs.open(new Path("/input/test.txt"));
            //构造一个输出流
            FileOutputStream fos=new FileOutputStream("D:\\javafile\\wordcount.txt");
            IOUtils.copyBytes(fin,fos,4096,true);
            //fin是输入源
            //fos输出地址
            //4096缓冲区的大小
            //是否关闭数据流
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void upload(){
        try {
            FSDataOutputStream fon=fs.create(new Path("/upload/test1.txt"),true);
            FileInputStream fis=new FileInputStream("D:\\javafile\\我的青春我做主.txt");
            IOUtils.copyBytes(fis,fon,4096,true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testMkdir(){
        try {
            boolean mkdir=fs.mkdirs(new Path("/upload/testmkdir"));
            System.out.println(mkdir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testDeldir(){
        try {
            boolean dedir=fs.delete(new Path("/upload/testmkdir"),true);
            System.out.println(dedir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testMvdir(){
        try {
            boolean dedir=fs.rename(new Path("/input/test.txt"),new Path("/input/donecount.txt"));
            System.out.println(dedir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
