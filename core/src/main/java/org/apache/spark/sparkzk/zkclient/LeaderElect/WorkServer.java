package org.apache.spark.sparkzk.zkclient.LeaderElect;


import org.apache.spark.sparkzk.zkclient.common.IZkDataListener;
import org.apache.spark.sparkzk.zkclient.common.ZkClient;
import org.apache.spark.sparkzk.zkclient.common.exception.*;
import org.apache.zookeeper.CreateMode;
import org.apache.spark.sparkzk.zkclient.common.serializer.ObTrans;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class WorkServer {

    //客户端状态
    private volatile boolean running = false;

    private ZkClient zkClient;

    //zk主节点路径
    public static final String MASTER_PATH = "/sparkMaster";

    //监听(用于监听主节点删除事件)
    private IZkDataListener dataListener;

    //服务器基本信息
    private RunningData serverData;
    //主节点基本信息
    private RunningData masterData;

    //调度器
    private ScheduledExecutorService delayExector = Executors.newScheduledThreadPool(1);
    //延迟时间5s
    private int delayTime = 2;

    private int masterTakeTimeMil = 500;

    public WorkServer(RunningData runningData, ZkClient zkClient,int masterTakeTimeMil){
        this.masterTakeTimeMil = masterTakeTimeMil;
        this.serverData = runningData;
        this.zkClient = zkClient;
        this.dataListener = new IZkDataListener() {
            @Override
            public void handleDataChange(String s, byte[] data) throws Exception {
                //here should update the master data

            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                //takeMaster();
                //若之前master为本机,则立即抢主,否则延迟5秒抢主(防止小故障引起的抢主可能导致的网络数据风暴)
                if(masterData != null && masterData.getName().equals(serverData.getName())){
                    takeMaster();
                }else{
                    try {
                        TimeUnit.SECONDS.sleep(delayTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    takeMaster();
                }
            }
        };
        if(running){
            try {
                throw new Exception("server has startup....");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        running = true;
        MonitorMasterNode monitorMasterNode = new MonitorMasterNode();
        Thread t1 = new Thread(monitorMasterNode);
        t1.start();

    }

    public String masterName(){
        return this.masterData.getName();
    }

    private class MonitorMasterNode implements Runnable{

        @Override
        public void run() {
            while(running){
                zkClient.subscribeDataChanges(MASTER_PATH,dataListener);
                takeMaster();
                try {
                    Thread.sleep(masterTakeTimeMil);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //stop master service
    public void stop(){
        if(!running){
            try {
                throw new Exception("server has stopped.....");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        running = false;
        delayExector.shutdown();
        zkClient.unsubscribeDataChanges(MASTER_PATH,dataListener);
        releaseMaster();
    }

    //take master vote
    private void takeMaster(){
        if(!running) return ;

        try {
            zkClient.create(MASTER_PATH, ObTrans.ObjectToBytes(serverData), CreateMode.EPHEMERAL);
            masterData = serverData;
            System.out.print("my name is "+serverData.getName());
            System.out.println(", I am the master");

            /*delayExector.schedule(new Runnable() {//测试抢主用,每5s释放一次主节点
                @Override
                public void run() {
                    if(checkMaster()){
                        releaseMaster();
                    }
                }
            },5,TimeUnit.SECONDS);*/


        }catch (ZkNodeExistsException e){//节点已存在
            RunningData runningData = (RunningData) ObTrans.BytesToObject(zkClient.readData(MASTER_PATH,true));
            if(runningData == null){//读取主节点时,主节点被释放
                takeMaster();
            }else{
                //System.out.println("my name is "+serverData.getName());
                masterData = runningData;
                //System.out.println(masterData.getName()+" is master");
            }
        } catch (Exception e) {
            // ignore;
        }

    }
    private void releaseMaster(){
        if(checkMaster()){
            zkClient.delete(MASTER_PATH);
        }
    }


    private boolean checkMaster(){
        try {
            RunningData runningData = (RunningData)ObTrans.BytesToObject(zkClient.readData(MASTER_PATH));
            masterData = runningData;
            if (masterData.getName().equals(serverData.getName())) {
                return true;
            }
            return false;

        }catch (ZkNoNodeException e){//节点不存在
            return  false;
        }catch (ZkInterruptedException e){//网络中断
            return checkMaster();
        }catch (Exception e){//其它
            return false;
        }
    }

}
