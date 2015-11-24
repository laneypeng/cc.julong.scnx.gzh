package com.apache.hbase.bulkimport;

import java.io.DataOutputStream;  
import java.io.IOException;  
import java.util.HashMap;  
import java.util.Iterator;  
  
  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataOutputStream;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.Writable;  
import org.apache.hadoop.io.WritableComparable;  
import org.apache.hadoop.io.compress.CompressionCodec;  
import org.apache.hadoop.io.compress.GzipCodec;  
import org.apache.hadoop.mapreduce.OutputCommitter;  
import org.apache.hadoop.mapreduce.RecordWriter;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.ReflectionUtils; 

/**
 * 自定义Mapreduce多文件输出
 * @author zhangfeng
 *
 * @param <K>
 * @param <V>
 */
public class MultipleOutputFormat<K extends WritableComparable<?>, V extends Writable> extends FileOutputFormat<K, V> {

	   //接口类，需要在调用程序中实现generateFileNameForKeyValue来获取文件名   
   
	
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		return null;
	}

}
