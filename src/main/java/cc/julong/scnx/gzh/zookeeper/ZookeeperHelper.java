package cc.julong.scnx.gzh.zookeeper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.*;

import org.apache.zookeeper.data.Stat;
import org.slf4j.LoggerFactory;

/**
 * zookeeper操作工具类
 *
 * Created by zhangfeng on 2015/11/24.
 */
public class ZookeeperHelper implements Watcher{
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ZookeeperHelper.class);
    AtomicInteger seq = new AtomicInteger();
    private static int SESSION_TIMEOUT = 10000;
    private static String CONNECTION_STRING = "dev02:2181,dev03:2181,dev04:2181";
    private static final String ZK_PATH 				= "/gzh";
    private static final String CHILDREN_PATH 	= "/gzh/ch";
    private static final String LOG_PREFIX_OF_MAIN = "【Main】";

    private ZooKeeper zk = null;

    private CountDownLatch connectedSemaphore = new CountDownLatch( 1 );

    public ZookeeperHelper(){
        this.CONNECTION_STRING = PropertiesHelper.getInstance().getValue("zookeeper.hosts");
        this.SESSION_TIMEOUT = Integer.parseInt(PropertiesHelper.getInstance().getValue("zookeeper.timeout"));
    }

    /**
     * 创建ZK连接
     */
    public void createConnection() {
        // this.releaseConnection();
        try {
            zk = new ZooKeeper( CONNECTION_STRING, SESSION_TIMEOUT,this );
            LOG.info( LOG_PREFIX_OF_MAIN + "开始连接ZK服务器" );
            connectedSemaphore.await();
        } catch ( Exception e ) {}
    }

    /**
     * 关闭ZK连接
     */
    public void releaseConnection() {
        try {
            this.zk.close();
        } catch ( InterruptedException e ) {}
    }

    /**
     * 收到来自Server的Watcher通知后的处理。
     */
    @Override
    public void process( WatchedEvent event ) {
    }

    /**
     *  创建节点
     * @param path 节点path
     * @param data 初始数据内容
     * @return
     */
    public boolean createPath( String path, String data ) {
        try {
            this.zk.exists( path, true );
            String nodePath = this.zk.create( path, //路径名称
                    data.getBytes(), //节点数据
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, //节点访问权限
                    CreateMode.PERSISTENT //节点类型(持久性)
                );
            LOG.info( LOG_PREFIX_OF_MAIN + "节点创建成功, Path: " + nodePath  + ", content: " + data );
            return true;
        } catch ( Exception e ) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 读取指定节点数据内容
     * @param path 节点path
     * @return
     */
    public String readData( String path, boolean needWatch ) {
        try {
            return new String( this.zk.getData( path, needWatch, null ) );
        } catch ( Exception e ) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 更新指定节点数据内容
     * @param path 节点path
     * @param data  数据内容
     * @return
     */
    public boolean writeData( String path, String data ) {
        try {
            Stat stat = this.zk.setData(path, data.getBytes(), -1);
            LOG.info( LOG_PREFIX_OF_MAIN + "更新数据成功，path：" + path + ", stat: " + stat );
            return true;
        } catch ( Exception e ) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 删除指定节点
     * @param path 节点path
     */
    public void deleteNode( String path ) {
        try {
            this.zk.delete( path, -1 );
            LOG.info( LOG_PREFIX_OF_MAIN + "删除节点成功，path：" + path );
        } catch ( Exception e ) {
           e.printStackTrace();
        }
    }

    /**
     * 删除指定节点
     * @param path 节点path
     */
    public Stat exists( String path, boolean needWatch ) {
        try {
            return this.zk.exists( path, needWatch );
        } catch ( Exception e ) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main( String[] args ) throws Exception{


        ZookeeperHelper sample = new ZookeeperHelper();
        //创建连接
        sample.createConnection();
        //创建节点并写入数据
        sample.createPath( ZK_PATH, "1,FSN_20151124,hdfs://julong/fsndata/20151124" ) ;
        Thread.sleep(3000);
        //读取数据
        sample.readData(ZK_PATH, true);
        //释放连接
        sample.releaseConnection();
    }
}
