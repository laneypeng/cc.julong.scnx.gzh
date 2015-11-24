package com.apache.hbase.bulkimport;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


import com.apache.hbase.query.util.PropertiesHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.security.UserGroupInformation;


/**
 * Created by zhangfeng on 2015/1/7.
 */
public class Driver {

	private final static int recordLength = 1644;

	private static final Logger LOG = Logger.getLogger(Driver.class);

	static {
        //加载snappy压缩本地库
        String libsnappy = PropertiesHelper.getInstance().getValue("hadoop.io.libsnappy");
		System.load(libsnappy);
	}


	public static enum MY_COUNTER {
		PARSE_ERRORS, INVALID_FIELD_LEN, NUM_MSGS
	};

	/**
	 * 创建hbase表，对表根据给定的startkey和endkey进行预分配region
	 *
	 * @param conf
	 * @param args
	 * @throws Exception
	 */
	private static void createTable(Configuration conf, String args[]) throws Exception {
		String coprocessorJar = PropertiesHelper.getInstance().getValue("hbase.coprocessor.jar");
		String className = PropertiesHelper.getInstance().getValue("hbase.coprocessor.className");
		int priority = Integer.parseInt(PropertiesHelper.getInstance().getValue("hbase.coprocessor.priority"));
        String startKey = PropertiesHelper.getInstance().getValue("hbase.region.startkey");
        String endKey = PropertiesHelper.getInstance().getValue("hbase.region.stopkey");
        String regionsNum =  PropertiesHelper.getInstance().getValue("hbase.region.number");

        HBaseAdmin admin = new HBaseAdmin(conf);
		// 判断表是否存在，如果不存在创建表，如果存在直接插入数据
		String nameSpaces = PropertiesHelper.getInstance().getValue("hbase.namespace");
        //hdfsRootPath,outPath,tableName,zkHosts,zkNodeParent,startKey,endKey,regionsNum,day
        String tableName = nameSpaces +":" +args[2];
		if (!admin.tableExists(tableName)) {
			// 设置预拆分表的region数量，startkey，endkey
			int numRegions = Integer.parseInt(regionsNum);
			// 创建表的column family
			HColumnDescriptor cf = new HColumnDescriptor("cf");
			// 设置数据的压缩方式为SNAPPY
			cf.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
			cf.setCompressionType(Compression.Algorithm.SNAPPY);
			// 创建表
			
			HTableDescriptor td = new HTableDescriptor(tableName);
			td.addFamily(cf);
			//给数据表指定coprocessor
			Path jarPath = new Path(coprocessorJar);
			td.addCoprocessor(className,jarPath, priority, null);
			admin.createTable(td, startKey.getBytes(), endKey.getBytes(),numRegions);
			
		}
	}
	/**
	 * 删除目录和目录下的文件及目录
	 * @param path
	 */
	public static void deleteAllFilesOfDir(File path) {
		if (!path.exists())
			return;
		if (path.isFile()) {
			path.delete();
			return;
		}
		File[] files = path.listFiles();
		for (int i = 0; i < files.length; i++) {
			deleteAllFilesOfDir(files[i]);
		}
		path.delete();
	}

	public static void main(String[] args) throws Exception {
        LOG.info("=============================================");
        LOG.info("hdfs input Path  : " + args[0]);
        LOG.info("hdfs output Path : " + args[1]);
        LOG.info("hbase table name : " + args[2]);
        LOG.info("import date      : " + args[3]);
        LOG.info("=============================================");

        String zkHosts = PropertiesHelper.getInstance().getValue("hbase.zookeeper.quorum");
        String zkNodeParent = PropertiesHelper.getInstance().getValue("zookeeper.znode.parent");
        //args = new String[]{"hdfs://julong/fsndata/zip/20150820","hdfs://julong/tmp/FSN_201508","FSN_201508",zkHosts,zkNode,"A","Z","3","20150820"};
        String tableName = args[2];
		Configuration config = new Configuration();

		config.addResource("core-site.xml");
		config.addResource("hdfs-site.xml");
		config.addResource("mapred-site.xml");
		config.addResource("yarn-site.xml");
		config.addResource("hbase-site.xml");

		config.set("hbase.zookeeper.quorum", zkHosts);
        config.set("zookeeper.znode.parent",zkNodeParent);

		// 创建一个Job
		Job job = new Job(config, "HBase Bulk Import Data ,table name : " + tableName);
		job.setWorkingDirectory(new Path("/tmp"));

		job.setJarByClass(HBaseKVMapper.class);
		job.setMapperClass(HBaseKVMapper.class);
		job.setInputFormatClass(ZipFileInputFormat.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);

		// 初始化表的reducer数
		TableMapReduceUtil.initTableReducerJob(tableName, null, job);
        //hdfsRootPath,outPath,tableName,zkHosts,zkNodeParent,startKey,endKey,regionsNum,day
		// 创建HBase的配置对象
		Configuration hbaseconfig = HBaseConfiguration.create();
		hbaseconfig.set("hbase.zookeeper.quorum", zkHosts);
		hbaseconfig.set("hbase.zookeeper.property.clientPort","2181");
		hbaseconfig.set("zookeeper.znode.parent", zkNodeParent);

		// 创建Hbase表，压缩方式是snappy
		createTable(hbaseconfig, args);
		String nameSpaces = PropertiesHelper.getInstance().getValue("hbase.namespace");
		String tn = nameSpaces +":" + tableName;
		
		// 构造HTable对象
		HTable hTable = new HTable(hbaseconfig, tn);

		// 自动配置partitioner and reducer
		HFileOutputFormat.configureIncrementalLoad(job, hTable);

		// 设置文件的输入输出路径
		String[] files = args[0].split(",");
		for (String file : files) {
			FileInputFormat.addInputPath(job, new Path(file));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getJar();
		// 等待Hfile文件生成完成
		job.waitForCompletion(true);
		// 获取job计数器中记录的数据
		Counters counters = job.getCounters();
		Counter separatorError = counters.findCounter(MY_COUNTER.PARSE_ERRORS);
        LOG.info("Separator error record number :" + separatorError.getValue());
		Counter fieldError = counters.findCounter(MY_COUNTER.INVALID_FIELD_LEN);
        LOG.info("Field error record number :" + fieldError.getValue());
		Counter records = counters.findCounter(MY_COUNTER.NUM_MSGS);
        LOG.info("Normal record number : " + records.getValue());
		// 装载hfile文件到HBase表中
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(config);
        LOG.info("hfile path : " + args[1]);
		loader.doBulkLoad(new Path(args[1]), hTable);

        try {
            String hdfsPrefix = PropertiesHelper.getInstance().getValue("hdfs.prefix");
            LOG.info("delete mapreduce out file : " + hdfsPrefix +"/tmp/" + tableName);
            HDFS.deleteFileDir( hdfsPrefix +"/tmp/" + tableName);

            String localDataDir = PropertiesHelper.getInstance().getValue("local.data.dir") ;
            //删除本地下载的文件目录
            LOG.info("delete local file : " + localDataDir + "/zip");
			deleteAllFilesOfDir(new File(localDataDir + "/zip"));

        } catch (IOException e) {
            e.printStackTrace();
        }
	}

}
