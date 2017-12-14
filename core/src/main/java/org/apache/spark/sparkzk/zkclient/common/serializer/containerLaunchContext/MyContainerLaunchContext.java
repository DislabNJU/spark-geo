package org.apache.spark.sparkzk.zkclient.common.serializer.containerLaunchContext;

import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.spark.sparkzk.zkclient.common.serializer.ObTrans;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by ubuntu2 on 9/21/17.
 */
public class MyContainerLaunchContext implements Serializable{

    private Map<MyApplicationAccessType, String> myApplciationACLs = new HashMap<MyApplicationAccessType, String>();
    private List<String> myCommond = new ArrayList<String>();
    private Map<String, String> myEnvi = new HashMap<String, String>();
    private Map<String, MyByteBuffer> myServiceData = new HashMap<String, MyByteBuffer>();
    private Map<String, MyLocalResource> myLocalResources = new HashMap<String, MyLocalResource>();
    private MyByteBuffer myByteBuffer;


    public class MyByteBuffer implements  Serializable{
        private byte[] buffer;
        public MyByteBuffer(byte []buffer){
            this.buffer = buffer.clone();
        }

        public void setBuffer(byte []buffer){
            this.buffer = buffer.clone();
        }

        public byte[] getBuffer(){
            return this.buffer;
        }
       // ByteBuffer byteBuffer1 = ByteBuffer.wrap(nye);
    }


    public MyContainerLaunchContext(ContainerLaunchContext containerLaunchContext){


        this.setApplicationACLs(containerLaunchContext.getApplicationACLs());
        this.setCommands(containerLaunchContext.getCommands());
        this.setEnvironment(containerLaunchContext.getEnvironment());
        this.setLocalResources(containerLaunchContext.getLocalResources());
        this.setServiceData(containerLaunchContext.getServiceData());
        this.setTokens(containerLaunchContext.getTokens());
    }

    public ContainerLaunchContext transBack(){
        ContainerLaunchContext ctx =
                Records.newRecord(ContainerLaunchContext.class);
        /*byte[] myByy = myByteBuffer.getBuffer();
        ByteBuffer buffer = ByteBuffer.allocate(myByy.length);
        buffer = ByteBuffer.wrap(myByy);
        System.out.println(buffer);*/

        ctx.setTokens(this.getTokens());
        ctx.setApplicationACLs(this.getApplicationACLs());
        ctx.setCommands(this.getCommands());
        ctx.setEnvironment(this.getEnvironment());
        ctx.setLocalResources(this.getLocalResources());
        ctx.setServiceData(this.getServiceData());
        return ctx;
    }

    public  ByteBuffer getTokens(){
        if(this.myByteBuffer == null) {
            return null;
        }
        return ByteBuffer.wrap(myByteBuffer.getBuffer());
    }

    public  void setTokens(ByteBuffer tokens){
        if(tokens == null) {

            this.myByteBuffer = null;
        }
        else {
            this.myByteBuffer = new MyByteBuffer(tokens.array());
        }
    }

    public  Map<String, LocalResource> getLocalResources(){
        if(this.myLocalResources == null){
            return null;
        }

        Map<String, LocalResource> localResourceMap = new HashMap<String, LocalResource>();
        Iterator<String> iter = this.myLocalResources.keySet().iterator();
        while (iter.hasNext()) {
            String name = iter.next();
            MyLocalResource temp = myLocalResources.get(name);
            localResourceMap.put(name,temp.transBack());
        }
        return localResourceMap;
    }

    public  void setLocalResources(Map<String, LocalResource> localResources) {
        if(localResources == null){
            this.myLocalResources = null;
        }
        else {
            Iterator<String> iter = localResources.keySet().iterator();
            while (iter.hasNext()) {
                String name = iter.next();
                LocalResource temp1 = localResources.get(name);
                MyLocalResource temp2 = MyLocalResource.newInstance(
                        temp1.getResource(), temp1.getType(), temp1.getVisibility(),
                        temp1.getSize(), temp1.getTimestamp(), temp1.getPattern()
                );
                this.myLocalResources.put(name, temp2);
            }
        }
    }

    public  Map<String, ByteBuffer> getServiceData(){
        if(this.myServiceData ==null){
            return null;
        }
        Map<String, ByteBuffer> byteBufferMap = new HashMap<String, ByteBuffer>();
        Iterator<String> iter = this.myServiceData.keySet().iterator();
        while(iter.hasNext()){
            String name = iter.next();
            MyByteBuffer temp = this.myServiceData.get(name);
            byteBufferMap.put(name,ByteBuffer.wrap(temp.getBuffer()));
        }
        return byteBufferMap;
    }

    public  void setServiceData(Map<String, ByteBuffer> serviceData){
        if(serviceData == null){
            this.myServiceData = null;
        }
        else {

            Iterator<String> iter = serviceData.keySet().iterator();
            while (iter.hasNext()) {
                String name = iter.next();
                ByteBuffer temp1 = serviceData.get(name);
                MyByteBuffer temp2 = new MyByteBuffer(temp1.array());
                this.myServiceData.put(name, temp2);
            }
        }

    }

    public  Map<String, String> getEnvironment(){
        return this.myEnvi;
    }

    public  void setEnvironment(Map<String, String> environment){
        if(environment == null){
            this.myEnvi = null;
        }
        else {
            this.myEnvi = new HashMap<String, String>(environment);
        }
    }

    public  List<String> getCommands(){
        return this.myCommond;
    }

    public  void setCommands(List<String> commands){
        if(commands == null){
            this.myCommond = null;
        }
        else {
            this.myCommond = new ArrayList<String>(commands);
        }
    }

    public   Map<ApplicationAccessType, String> getApplicationACLs(){
        if(this.myApplciationACLs == null){
            return null;
        }
        Map<ApplicationAccessType, String> tempMap = new HashMap<ApplicationAccessType, String>();
        Iterator<MyApplicationAccessType> iter = this.myApplciationACLs.keySet().iterator();
        while(iter.hasNext()){
            MyApplicationAccessType name = iter.next();
            String temp = this.myApplciationACLs.get(name);
            ApplicationAccessType nameTrue = ApplicationAccessType.MODIFY_APP;
            if(name == MyApplicationAccessType.MODIFY_APP){
                nameTrue = ApplicationAccessType.MODIFY_APP;
            }
            else if(name == MyApplicationAccessType.VIEW_APP){
                nameTrue = ApplicationAccessType.VIEW_APP;
            }
            tempMap.put(nameTrue,temp);
        }
        return tempMap;
    }

    public   void setApplicationACLs(Map<ApplicationAccessType, String> acls){
        if(acls == null){
            this.myApplciationACLs = null;
        }
        else {
            Iterator<ApplicationAccessType> iter = acls.keySet().iterator();
            while (iter.hasNext()) {
                ApplicationAccessType aat = iter.next();
                String temp = acls.get(aat);
                if (aat == ApplicationAccessType.MODIFY_APP) {
                    this.myApplciationACLs.put(MyApplicationAccessType.MODIFY_APP, temp);
                } else if (aat == ApplicationAccessType.VIEW_APP) {
                    this.myApplciationACLs.put(MyApplicationAccessType.VIEW_APP, temp);


                }
            }
        }
    }

}
