package org.apache.spark.sparkzk.zkclient.LeaderElect;
import java.io.Serializable;

/**
 * Created by nevermore on 16/6/22.
 */
public class RunningData implements Serializable {

    private static final long serialVersionUID = 4260577459043203630L;


    private String cid;
    private String name;
    final private String clientTag = "client@";
    public RunningData(String cid){
        this.cid = cid;
        this.name = clientTag+cid;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public String getName() {
        return this.name = clientTag+cid;
    }

    public void setName(String name) {
        this.name = name;
    }
}