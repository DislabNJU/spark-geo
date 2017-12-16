package org.apache.spark.sparkzk.zkclient;

import org.apache.spark.sparkzk.zkclient.common.AmRecoveryHostData;
import org.apache.spark.sparkzk.zkclient.common.IZkClient;
import org.apache.spark.sparkzk.zkclient.common.IZkDataListener;
import org.apache.spark.sparkzk.zkclient.common.ZkClient;
import org.apache.spark.sparkzk.zkclient.common.serializer.ObTrans;

import java.util.ArrayList;
import java.util.List;


public class ZkSparkRecoveryCentre {
    public static String dataRootPath = "/sparkRecoveryCentre";
    private IZkClient zk;
    private String myRecoveryPath = null;
    private String myNodeName = null;
    private String processDoneNode = "false";
    public ZkSparkRecoveryCentre(String hostName, String appNodeName){
        myNodeName = appNodeName;
        String path = dataRootPath+"/"+appNodeName;
        myRecoveryPath = path;

        zk = new ZkClient(hostName);
        if(!zk.exists(dataRootPath)) {
            zk.createPersistent(dataRootPath);
        }

        if(!zk.exists(path)) {
            zk.createPersistent(path);
        }
        else{
            System.out.println("create a node: "+path+" which has been exit!");
        }
    }

    /*public String  getAppNodeName(){
        return myNodeName;
    }

    public String getAppRecoveryPath(){
        return myRecoveryPath;
    }
    */

    public void putRecoveryTask(String yarnHostName, String timeAdd){
        AmRecoveryHostData amRecoveryHostData = new AmRecoveryHostData(yarnHostName);
        String path = myRecoveryPath+"/" + timeAdd;
        zk.createEphemeral(path, ObTrans.ObjectToBytes(amRecoveryHostData));
    }

    //return true recovery child exist
    public boolean isRecoveryChildExist(){
        return !zk.getChildren(myRecoveryPath).isEmpty();
    }

    public List<String> getAllRecoveryHost(){
        //get the recovery am rm host, and delete the dealed one
        List<String> recoveryAmHostList = new ArrayList<String>();
        List<String> childrenName = zk.getChildren(myRecoveryPath);

        for(String childName : childrenName){
            String dataPath = myRecoveryPath+"/"+childName;
            AmRecoveryHostData amRecoveryHostData =
                    (AmRecoveryHostData)ObTrans.BytesToObject(zk.readData(dataPath));
            recoveryAmHostList.add(amRecoveryHostData.getRecoveryAmRmHost());
            //has del with it, delete it
            zk.delete(dataPath);
        }

        return recoveryAmHostList;
    }

    public void setProcessDone(){
        String donePath = myRecoveryPath+"/"+processDoneNode;
        this.zk.createEphemeral(donePath);
    }
    //return true process done
    public boolean isProcessDone(){
        return zk.getChildren(myRecoveryPath).contains(processDoneNode);
    }

}
