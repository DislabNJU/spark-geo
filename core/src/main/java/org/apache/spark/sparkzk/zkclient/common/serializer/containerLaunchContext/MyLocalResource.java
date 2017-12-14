package org.apache.spark.sparkzk.zkclient.common.serializer.containerLaunchContext;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 9/22/17.
 */
public class MyLocalResource implements Serializable {

    private MyURL myURL;
    private MyLocalResourceType myLocalResourceType;
    private MyLocalResourceVisibility myLocalResourceVisibility;
    private long size;
    private long timestamp;
    private String pattern;

    public static MyLocalResource newInstance(URL url, LocalResourceType type,
                                              LocalResourceVisibility visibility, long size, long timestamp,
                                              String pattern) {
        MyLocalResource resource = new MyLocalResource();
        resource.setResource(url);
        resource.setType(type);
        resource.setVisibility(visibility);
        resource.setSize(size);
        resource.setTimestamp(timestamp);
        resource.setPattern(pattern);
        return resource;
    }
    public static MyLocalResource newInstance(URL url, LocalResourceType type,
                                              LocalResourceVisibility visibility, long size, long timestamp) {
        return newInstance(url, type, visibility, size, timestamp, null);
    }

    public LocalResource transBack(){
        return LocalResource.newInstance(
                this.getResource(),this.getType(),this.getVisibility(),this.size,this.timestamp,this.pattern);
    }

    public URL getResource(){
        return this.myURL.transBack();
    }


    public  void setResource(URL resource){
        this.myURL = MyURL.newInstance(resource.getScheme(),resource.getHost(),
                resource.getPort(),resource.getFile());
    }


    public  long getSize(){
        return this.size;
    }


    public  void setSize(long size){
        this.size = size;
    }


    public  long getTimestamp(){
        return this.timestamp;
    }


    public  void setTimestamp(long timestamp){
        this.timestamp = timestamp;
    }


    public LocalResourceType getType(){
        if(myLocalResourceType == MyLocalResourceType.ARCHIVE){
            return LocalResourceType.ARCHIVE;
        }
        else if(myLocalResourceType == MyLocalResourceType.FILE){
            return LocalResourceType.FILE;
        }
        else if(myLocalResourceType == MyLocalResourceType.PATTERN){
            return LocalResourceType.PATTERN;
        }
        else{
            return null;
        }

    }


    public  void setType(LocalResourceType type){
        if(type == LocalResourceType.ARCHIVE){
            this.myLocalResourceType = MyLocalResourceType.PATTERN;
        }
        else if(type == LocalResourceType.FILE){
            this.myLocalResourceType = MyLocalResourceType.FILE;

        }
        else if(type == LocalResourceType.PATTERN){
            this.myLocalResourceType = MyLocalResourceType.ARCHIVE;

        }

    }


    public LocalResourceVisibility getVisibility(){
        if(myLocalResourceVisibility == MyLocalResourceVisibility.APPLICATION){
            return LocalResourceVisibility.APPLICATION;
        }
        else if(myLocalResourceVisibility == MyLocalResourceVisibility.PRIVATE){
            return LocalResourceVisibility.PRIVATE;
        }
        else if(myLocalResourceVisibility == MyLocalResourceVisibility.PUBLIC){
            return LocalResourceVisibility.PUBLIC;
        }
        else{
            return null;
        }
    }


    public  void setVisibility(LocalResourceVisibility visibility){
        if(visibility == LocalResourceVisibility.APPLICATION){
            this.myLocalResourceVisibility = MyLocalResourceVisibility.APPLICATION;
        }
        else if(visibility == LocalResourceVisibility.PRIVATE){
            this.myLocalResourceVisibility = MyLocalResourceVisibility.PRIVATE;

        }
        else if(visibility == LocalResourceVisibility.PUBLIC){
            this.myLocalResourceVisibility = MyLocalResourceVisibility.PUBLIC;
        }
    }


    public  String getPattern(){
        return this.pattern;
    }


    public  void setPattern(String pattern){
        this.pattern = pattern;
    }
}
