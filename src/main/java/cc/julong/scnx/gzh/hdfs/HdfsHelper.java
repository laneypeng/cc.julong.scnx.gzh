package cc.julong.scnx.gzh.hdfs;

import cc.julong.scnx.gzh.zookeeper.ZookeeperHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.URI;


/**
 * HDFS操作工具类
 * Created by zhangfeng on 2015/11/24.
 */
public class HdfsHelper {

    /**
     * 删除HDFS上的文件
     * @param hdfsPath 要删除的hdfs上的文件路径
     * @throws java.io.IOException
     */
    public static void deleteFileDir(String hdfsPath)
            throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);
        Path dstPath = new Path(hdfsPath);
        fs.delete(dstPath, true);
    }

    /**
     * 复制本地目录下的数据上传到HDFS上指定目录下，如果HDFS上的目录不存在，自动创建目录
     * @param conf
     * @param localPath
     * @param hdfsPath
     */
    public static void copyFromLocal(Configuration conf,String localPath,String hdfsPath){
        try {
            FileSystem fs = FileSystem.get(conf);
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

    /**
     * 在HDFS上创建目录
     * @param conf
     * @param hdfsPath
     */
    public static void mkdir(Configuration conf,String hdfsPath){
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
            if(!fs.exists(new Path(hdfsPath))) {
                fs.mkdirs(new Path(hdfsPath));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //创建配置文件
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        //本地文件路径，该路径下直接是fsn zip文件，不在有文件夹
        String localPath = "/fsn/20151124";
        //数据上传到HDFS上的路径
        String hdfsPath = "hdfs://julong/fsndata/20151124";
        HdfsHelper.copyFromLocal(conf, localPath, hdfsPath);

        //写数据到Zookeeper节点
        ZookeeperHelper sample = new ZookeeperHelper();
        //创建连接
        sample.createConnection();
        //创建节点并写入数据
        sample.createPath("/gzh", "1,FSN_20151124,hdfs://julong/fsndata/20151124");
        //释放连接
        sample.releaseConnection();


    }
}
