package com.apache.hbase.bulkimport;


import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;


/**
 * hdfs工具类，删除文件
 * @author zhangfeng
 *
 */
public class HDFS {
	
	/**
	 * 删除HDFS上的文件
	 * @param hdfsPath 要删除的hdfs上的文件路径
	 * @throws java.io.IOException
	 */
	public static void deleteFileDir(String hdfsPath)
			throws IOException {
		Configuration conf = new Configuration();
		//这个地方skyform替换成自己core-site.xml文件中的fs.defaultFS向对应的值
	    FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);  
		Path dstPath = new Path(hdfsPath);
		fs.delete(dstPath, true);
	}

    public static void copyFromLocal(String localPath,String hdfsPath){
        Configuration conf = new Configuration();
        try {

            FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
            if(!fs.exists(new Path(hdfsPath))) {
                fs.mkdirs(new Path(hdfsPath));
            }
            File file = new File(localPath);
            for(File f :file.listFiles()) {
                fs.copyFromLocalFile(new Path(f.getAbsolutePath()), new Path(hdfsPath));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static void mkdir(String hdfsPath){
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
            if(!fs.exists(new Path(hdfsPath))) {
                fs.mkdirs(new Path(hdfsPath));
            }
         }catch (Exception e){
            e.printStackTrace();
        }
    }

	/**
	 * 下载HDFS上的文件到本地
	 * @param hdfsPath HDFS文件路径
	 * @param localFilePath 本地存放路径
	 * @throws java.io.IOException
	 */
	public static void downFileToLocal(String hdfsPath, String localFilePath)
			throws IOException {
		Configuration conf = new Configuration();
		//这个地方skyform替换成自己core-site.xml文件中的fs.defaultFS向对应的值
	    FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);  
		Path dstPath = new Path(hdfsPath);
		FSDataInputStream in = fs.open(dstPath);
		OutputStream out = new FileOutputStream(new File(localFilePath));
		IOUtils.copy(in, out);
	}


	public static void main(String args[]) throws IOException{
		//这里是你的要在的HDFS上的额文件路径
//		String hdfsPath = "hdfs://hadoop01:9000/input/hadoop-root-namenode-hadoop01.out.4";
        String local = "D:\\fsntest\\20150825\\zip\\20150825\\34_21_5691_20150820173703352.zip";
        copyFromLocal(local,"hdfs://julong/fsndata/zip/20150825/");

//		String localPath = "d:\\0028_0.txt";
//		downFileToLocal(hdfsPath,localPath);
//        deleteFileDir(hdfsPath);
		write("777777777777777777777777");
	}
	
	
	public static void write(String record){
		 Configuration conf = new Configuration();  
         String serverPath = "hdfs://hadoop01:9000/log.txt";  
	        Path hdfsfile = new Path(serverPath);  
	        FileSystem hdfs = null;
			try {
				hdfs = FileSystem.get(URI.create(serverPath), conf);
		        //根据上面的serverPath，获取到的是一个org.apache.hadoop.hdfs.DistributedFileSystem对象  
		        FSDataOutputStream out = hdfs.create(hdfsfile,false);  
	            out.writeUTF(record+"\n");  
		        out.close();  

			} catch (Exception e) {
				e.printStackTrace();
			}  
	}
	
	

}
