package org.apache.spark.sparkzk.zkclient.common.serializer.containerLaunchContext;

import org.apache.hadoop.yarn.api.records.URL;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 9/24/17.
 */
public class MyURL implements Serializable {

    private String scheme;
    private String host;
    private int port;
    private String file;

    public static MyURL newInstance(String scheme, String host, int port, String file) {
        MyURL url = new MyURL();
        url.setScheme(scheme);
        url.setHost(host);
        url.setPort(port);
        url.setFile(file);
        return url;
    }

    public URL transBack(){
        return URL.newInstance(this.scheme,this.host,this.port,this.file);
    }

    public  String getScheme(){

        return this.scheme;
    }

    public  void setScheme(String scheme){
        this.scheme = scheme;
    }

    /*public  String getUserInfo(){
        return this.
    }


    public  void setUserInfo(String userInfo){

    }
*/
    public  String getHost(){

        return this.host;
    }

    public  void setHost(String host){
        this.host = host;
    }
    public  int getPort(){

        return this.port;
    }
    public  void setPort(int port){
        this.port = port;
    }
    public  String getFile(){

        return this.file;
    }
    public  void setFile(String file){
        this.file = file;
    }
}
