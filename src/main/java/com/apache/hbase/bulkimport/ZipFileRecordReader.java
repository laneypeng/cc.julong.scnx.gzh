package com.apache.hbase.bulkimport;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ZipFileRecordReader extends RecordReader<Text, BytesWritable> {
	private FSDataInputStream fsin;
	private ZipInputStream zip;
	private Text currentKey;
	private BytesWritable currentValue;
	private boolean isFinished = false;

	private ZipEntry zipEntry = null;
	private DataInputStream fsnin;
	private String zipFileName = "";
	
	private Path file;
	
	private long index = 0l;

	public void initialize(InputSplit inputSplit,
			TaskAttemptContext taskAttemptContext) throws IOException,
			InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		Configuration job = taskAttemptContext.getConfiguration();
		file = split.getPath();
		this.zipFileName = file.getParent().toString() + "/" + file.getName();
		FileSystem fs = file.getFileSystem(job);
		this.fsin = fs.open(split.getPath());
		this.zip = new ZipInputStream(this.fsin);
		this.zipEntry = this.zip.getNextEntry();
		//构建zip文件您输入流
		this.fsnin = new DataInputStream(new BufferedInputStream(this.zip));
		//跳过开始的32个字节，这些字节主要是fsn文件的版本，大小、记录总数等信息
		byte[] head = new byte[32];
		this.fsnin.read(head, 0, 32);
		this.index = 132;
	}

	/**
	 * 获取下一个KeyValue对
	 */
	public boolean nextKeyValue() throws IOException, InterruptedException {
		//如果
		if (this.zipEntry != null) {
			byte[] temp = new byte[1644];
			int eof = this.fsnin.read(temp, 0, 1644);
			this.currentKey = new Text(zipFileName + "#" + this.index + "#" + file.getName());
			if (eof != -1) {
				this.currentValue = new BytesWritable(temp);
				this.index += 1644;
				return true;
			}
			this.zipEntry = this.zip.getNextEntry();
			if (this.zipEntry != null) {
				byte[] head = new byte[32];
				this.fsnin.read(head, 0, 32);
				return true;
			}
			

			return false;
		}
		this.isFinished = true;
		return false;
	}

	public float getProgress() throws IOException, InterruptedException {
		return this.isFinished ? 1 : 0;
	}

	public Text getCurrentKey() throws IOException, InterruptedException {
		return this.currentKey;
	}

	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		return this.currentValue;
	}

	public void close() throws IOException {
		try {
			this.zip.close();
		} catch (Exception localException) {
		}
		try {
			this.fsin.close();
		} catch (Exception localException1) {
		}
	}
}