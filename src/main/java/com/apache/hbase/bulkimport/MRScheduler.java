package com.apache.hbase.bulkimport;

import java.io.File;
import java.io.IOException;

import cc.julong.ms.query.util.PropertiesHelper;
import cc.julong.ms.util.Shell;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;


/**
 * Created by zhangfeng on 2015/4/17.
 */
public class MRScheduler {

        private static final Logger LOG = Logger.getLogger(MRScheduler.class);


        public static void schedule(String appName,String day) {
            //截取到月
            String mouth = day.substring(0,6);
            //表名
            String tableName = "FSN_" + mouth;

            String work_Dir = PropertiesHelper.getInstance().getValue("work.dir");
            String localDataDir = PropertiesHelper.getInstance().getValue("local.data.dir") ;
            localDataDir = localDataDir +  "/zip/" + day + "/";
            LOG.info("Local data file path :  " + localDataDir + "'");
            File workDir = new File(work_Dir);

            String userName = PropertiesHelper.getInstance().getValue("securityload.username");
            String keytabpath = PropertiesHelper.getInstance().getValue("securityload.keytabpath");
//            String keytabpath = MRScheduler.class.getResource("/gzhglxt.keytab").toString();
            LOG.info("Keytab ============ " + keytabpath);
            try{
                if(userName != null && keytabpath != null){
                    UserGroupInformation.loginUserFromKeytab(userName, keytabpath);
                }
            } catch (Exception e){
                e.printStackTrace();
            }

            //复制本地文件到HDFS
            String hdfsRootPath = PropertiesHelper.getInstance().getValue("hdfs.data.root");
            hdfsRootPath += "/zip/"+ day;
            HDFS.copyFromLocal(localDataDir,hdfsRootPath);
            //调度MR运行进行数据导入
            //filePath : hdfs root path + "/zip/20150817"
            //$filepath /tmp/$tableName $tableName $zk $zkport $zk_node_parent  $startKey $endKey $numRegions $day
            String inputFilePath = hdfsRootPath;
            LOG.info("hdfs input path :" + inputFilePath);
            String hdfsPrefix = PropertiesHelper.getInstance().getValue("hdfs.prefix");
            String outPath = hdfsPrefix + "/tmp/" + tableName;
            LOG.info("hdfs output path :" + outPath);

            //String[] args = new String[]{inputFilePath,outPath,tableName,zkHosts,zkNodeParent,startKey,endKey,regionsNum,day};
            try {
                //"hdfs://julong/fsndata/zip/20150820","hdfs://julong/tmp/FSN_201508","FSN_201508",zkHosts,zkNode,"A","Z","3","20150820"};
                Shell.ShellCommandExecutor executor = new Shell.ShellCommandExecutor(new String[] { "sh", "-c" ,
            		work_Dir + "/hbase-bulk-load.sh " + inputFilePath +  " " + outPath + " " + tableName + " "+ day },workDir);
            try {
                executor.execute();
                LOG.info("Shell output: " + executor.getOutput());
            } catch (IOException e) {
              LOG.error("Shell Exception, " + e);
              e.printStackTrace();
              throw new RuntimeException("MRScheduler error, appName= " + appName, e);
            }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

}
