package cc.julong.scnx.gzh.zookeeper;


import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import cc.julong.scnx.gzh.shell.Shell;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 数据导入监控服务
 * <p/>
 * Created by zhangfeng on 2015/11/24.
 */
public class MonitorServer implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(MonitorServer.class);
    AtomicInteger seq = new AtomicInteger();
    private static int SESSION_TIMEOUT = 10000;
    private static String CONNECTION_STRING = "dpnode03:2181,dpnode04:2181,dpnode05:2181";
    private static final String ZK_PATH = "/gzh";
    private static final String LOG_PREFIX_OF_MAIN = "【Main】";

    private ZooKeeper zk = null;

    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public MonitorServer() {
        this.CONNECTION_STRING = PropertiesHelper.getInstance().getValue("zookeeper.hosts");
        this.SESSION_TIMEOUT = Integer.parseInt(PropertiesHelper.getInstance().getValue("zookeeper.timeout"));
    }

    /**
     * 创建ZK连接
     *
     * @param connectString  ZK服务器地址列表
     * @param sessionTimeout Session超时时间
     */
    public void createConnection(String connectString, int sessionTimeout) {
        // this.releaseConnection();
        try {
            zk = new ZooKeeper(connectString, sessionTimeout, this);
            LOG.info(LOG_PREFIX_OF_MAIN + "开始连接ZK服务器");
            connectedSemaphore.await();
        } catch (Exception e) {
        }
    }

    /**
     * 关闭ZK连接
     */
    public void releaseConnection() {
        try {
            this.zk.close();
        } catch (InterruptedException e) {
        }
    }

    /**
     * 收到来自Server的Watcher通知后的处理。
     */
    @Override
    public void process(WatchedEvent event) {

        // 连接状态
        KeeperState keeperState = event.getState();
        // 事件类型
        EventType eventType = event.getType();
        // 受影响的path
        String path = event.getPath();
        String logPrefix = "【Watcher-" + this.seq.incrementAndGet() + "】";

        LOG.info(logPrefix + "收到Watcher通知");
        LOG.info(logPrefix + "连接状态:\t" + keeperState.toString());
        LOG.info(logPrefix + "事件类型:\t" + eventType.toString());

        if (KeeperState.SyncConnected == keeperState) {
            // 成功连接上ZK服务器
            if (EventType.None == eventType) {
                LOG.info(logPrefix + "成功连接上ZK服务器");
                connectedSemaphore.countDown();
            } else if (EventType.NodeCreated == eventType) {
                LOG.info(logPrefix + "节点创建");

                this.exists(path, true);
                execJob();
            } else if (EventType.NodeDataChanged == eventType) {
                LOG.info(logPrefix + "节点数据更新");
                LOG.info(logPrefix + "数据内容: " + this.readData(ZK_PATH, true));
            } else if (EventType.NodeDeleted == eventType) {
                LOG.info(logPrefix + "节点 " + path + " 被删除");
            }

        } else if (KeeperState.Disconnected == keeperState) {
            LOG.info(logPrefix + "与ZK服务器断开连接");
        } else if (KeeperState.AuthFailed == keeperState) {
            LOG.info(logPrefix + "权限检查失败");
        } else if (KeeperState.Expired == keeperState) {
            LOG.info(logPrefix + "会话失效");
        }

        LOG.info("--------------------------------------------");

    }

    /**
     * 创建节点
     *
     * @param path 节点path
     * @param data 初始数据内容
     * @return
     */
    public boolean createPath(String path, String data) {
        try {
            this.zk.exists(path, true);
            LOG.info(LOG_PREFIX_OF_MAIN + "节点创建成功, Path: "
                    + this.zk.create(path, //
                    data.getBytes(), //
                    Ids.OPEN_ACL_UNSAFE, //
                    CreateMode.PERSISTENT)
                    + ", content: " + data);
        } catch (Exception e) {
        }
        return true;
    }

    /**
     * 读取指定节点数据内容
     *
     * @param path 节点path
     * @return
     */
    public String readData(String path, boolean needWatch) {
        try {
            return new String(this.zk.getData(path, needWatch, null));
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 更新指定节点数据内容
     *
     * @param path 节点path
     * @param data 数据内容
     * @return
     */
    public boolean writeData(String path, String data) {
        try {
            LOG.info(LOG_PREFIX_OF_MAIN + "更新数据成功，path：" + path + ", stat: " +
                    this.zk.setData(path, data.getBytes(), -1));
        } catch (Exception e) {
        }
        return false;
    }

    /**
     * 删除指定节点
     *
     * @param path 节点path
     */
    public void deleteNode(String path) {
        try {
            this.zk.delete(path, -1);
            LOG.info(LOG_PREFIX_OF_MAIN + "删除节点成功，path：" + path);
        } catch (Exception e) {
            //TODO
        }
    }

    /**
     * 判断节点是否存在
     *
     * @param path 节点path
     */
    public Stat exists(String path, boolean needWatch) {
        try {
            return this.zk.exists(path, needWatch);
        } catch (Exception e) {
            return null;
        }
    }


    public void deleteAllPath() {
        this.deleteNode(ZK_PATH);
    }

    private void execJob() {
        String workDir = PropertiesHelper.getInstance().getValue("work.dir");
        LOG.info("work.dir : " + workDir);
        String zkhosts = PropertiesHelper.getInstance().getValue("zookeeper.hosts");
        LOG.info("zkhosts : " + zkhosts);
        //读取数据
        String data = this.readData(ZK_PATH, true);
        LOG.info("data ===  " + data);
        if(data != null && !data.equals("")){
            String[] inputs = data.split(",");
            //判断文件数量是否一致，如果文件数量一致，提交MR任务执行

            String inputFile = inputs[2];
            String tableName = inputs[1];
            //运行任务脚本
            Shell.ShellCommandExecutor executor = new Shell.ShellCommandExecutor(new String[] { "sh", "-c",
                    workDir + "/bulkload.sh " + inputFile +  " " + tableName + " " + zkhosts},new File(workDir));
            try {
                executor.execute();
                LOG.info("executor result : " + executor.getOutput());
            } catch (IOException e) {
                e.printStackTrace();
            }
            //清理节点
            this.deleteAllPath();
        }
        this.deleteAllPath();
    }


    public static void main(String[] args) throws Exception {
        MonitorServer sample = new MonitorServer();
        while (true) {
            sample.createConnection(CONNECTION_STRING, SESSION_TIMEOUT);
            sample.execJob();
            String sleep = PropertiesHelper.getInstance().getValue("monitor.sleep.time");
            if(sleep != null){
                Thread.sleep(Integer.parseInt(sleep) * 3600);
            } else {
                Thread.sleep(30 * 3600);
            }

            sample.releaseConnection();
        }
    }


}

