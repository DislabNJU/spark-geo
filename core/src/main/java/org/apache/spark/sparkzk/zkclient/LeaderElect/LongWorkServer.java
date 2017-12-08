package org.apache.spark.sparkzk.zkclient.LeaderElect;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.spark.sparkzk.zkclient.common.IZkDataListener;
import org.apache.spark.sparkzk.zkclient.common.ZkClient;
import org.apache.spark.sparkzk.zkclient.common.exception.ZkNodeExistsException;
import org.apache.spark.sparkzk.zkclient.common.serializer.ObTrans;


/**
 *
 */
public class LongWorkServer {
    private volatile boolean running = false;

    private ZkClient zkClient;
    public static final String MasterRootNodeName = "sparkMaster";
    public static final String MasterRootPath = "/sparkMaster";
    public static final String masterNodeName = "master";
    public  String masterNodePath = null;//MasterRootPath+app+master
    public String nodePath = null;//MasterRootPath+app
    private RunningData serverData;
    private RunningData masterData;
    private Long masterSelectorNum = Long.valueOf(0);
    private ReentrantLock masterSelectorNumLock = new ReentrantLock();
    private IZkDataListener dataListener;
    private int takeMasterDelayTime = 3;
    private String appNodeName;


    public LongWorkServer(RunningData runningData, ZkClient zkClient, String appNodeName, Long masterSelectorNum) {
        running = true;
        this.serverData = runningData;
        this.appNodeName = appNodeName;
        setMasterSelectorNum(masterSelectorNum);
        this.zkClient = zkClient;
        if(!this.zkClient.exists(MasterRootPath)){
            zkClient.createPersistent(MasterRootPath);
        }
        String appNodePath = MasterRootPath+"/"+appNodeName;
        this.nodePath = appNodePath;
        if(!this.zkClient.exists(appNodePath)){
            zkClient.createPersistent(appNodePath);
        }

        this.masterNodePath = appNodePath+"/"+masterNodeName;

        if(!this.zkClient.exists(masterNodePath)){
            zkClient.createEphemeral(masterNodePath,ObTrans.ObjectToBytes(this.serverData));
            masterData = serverData;
            System.out.println("I am the master!"+"cid: "+serverData.getCid());

        }else{
            RunningData nowMasterData = (RunningData) ObTrans.BytesToObject(zkClient.readData(masterNodePath,true));
            masterData = nowMasterData;
        }

        MonitorMaserNode monitorMaserNode = new MonitorMaserNode();
        Thread t1 = new Thread(monitorMaserNode);
        t1.start();
    }

    public void setMasterSelectorNum(Long masterSelectorNum){
        masterSelectorNumLock.lock();
        this.masterSelectorNum = masterSelectorNum;
        masterSelectorNumLock.unlock();
    }

    public boolean isMasterExist(){
        return zkClient.exists(masterNodePath);
    }

    public boolean iAmMaster(){
        return serverData.getCid().equals(masterData.getCid());
    }

    public int masterId(){
        return Integer.parseInt(masterData.getCid());
    }

    private class MonitorMaserNode implements  Runnable{

        @Override
        public void run() {
            dataListener = new IZkDataListener() {
                @Override
                public void handleDataChange(String s, byte[] data) throws Exception {

                }

                @Override
                public void handleDataDeleted(String s) throws Exception {
                    //takeMaster();
                    //若之前master为本机,则立即抢主,否则延迟抢主(防止小故障引起的抢主可能导致的网络数据风暴)
                    if(masterData != null && masterData.getCid().equals(serverData.getCid())){
                        zkClient.createEphemeral(masterNodePath,ObTrans.ObjectToBytes(serverData));
                        masterData = serverData;
                        System.out.println("I am the master!"+"cid: "+serverData.getCid());
                    }else{
                        try {
                            TimeUnit.SECONDS.sleep(takeMasterDelayTime);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        electMaster();
                    }
                }
            };
            while(running){//?
                zkClient.subscribeDataChanges(masterNodePath,dataListener);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private void electMaster(){
        String myElectorClientNum = Long.toString(masterSelectorNum);
        String myElectorClientPath = nodePath+"/"+myElectorClientNum;
        zkClient.createEphemeral(myElectorClientPath,ObTrans.ObjectToBytes(this.serverData));
        try {
            TimeUnit.SECONDS.sleep(3);//wait for the other client to go in
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        List<String> rootMasterChildren = zkClient.getChildren(nodePath);
        Collections.sort(rootMasterChildren, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                // larger to min
                return o2.compareTo(o1);
            }
        });

        System.out.println(rootMasterChildren);
        System.out.println("My cid: "+serverData.getCid());

        if(rootMasterChildren.contains(masterNodeName)){
            RunningData runningData = (RunningData) ObTrans.BytesToObject(zkClient.readData(masterNodePath,true));
            zkClient.delete(myElectorClientPath);
            if(runningData == null){//读取主节点时,主节点被释放
                electMaster();
            }else{
                System.out.println("my name is "+serverData.getCid());
                masterData = runningData;
                System.out.println(masterData.getCid()+" is master");
            }
            return;
        }


        if(rootMasterChildren.get(0).equals(myElectorClientNum)){

            try {
                zkClient.createEphemeral(masterNodePath, ObTrans.ObjectToBytes(this.serverData));
                masterData = serverData;
                System.out.println("I am the master!"+"cid: "+serverData.getCid());

            } catch (ZkNodeExistsException e){//节点已存在
                RunningData runningData = (RunningData) ObTrans.BytesToObject(zkClient.readData(masterNodePath,true));
                zkClient.delete(myElectorClientPath);
                if(runningData == null){//读取主节点时,主节点被释放
                    electMaster();
                }else{
                    System.out.println("my name is "+serverData.getCid());
                    masterData = runningData;
                    System.out.println(masterData.getCid()+" is master");
                }
            }
            catch (Exception e) {
                // ignore;
            }

        }
        else{
            try {
                TimeUnit.SECONDS.sleep(2);//wait for the winner to register the master!
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            RunningData runningData = (RunningData) ObTrans.BytesToObject(zkClient.readData(masterNodePath,true));
            zkClient.delete(myElectorClientPath);
            if(runningData == null){//读取主节点时,主节点被释放
                electMaster();
            }else{
                System.out.println("my name is "+serverData.getCid());
                masterData = runningData;
                System.out.println(masterData.getCid()+" is master");
            }
        }

    }
}


