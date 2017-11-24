/**
 *
 */
package org.apache.spark.sparkzk.zkclient.common;

import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.util.List;

/**
 * An abstract class for zookeeper listner
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-12-4
 * @see IZkChildListener
 * @see IZkDataListener
 * @see IZkStateListener
 */
public abstract class AbstractListener implements IZkChildListener, IZkDataListener, IZkStateListener {

    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
    }

    @Override
    public void handleNewSession() throws Exception {
    }

    @Override
    public void handleDataChange(String dataPath, byte[] data) throws Exception {
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
    }

}
