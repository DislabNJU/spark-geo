package org.apache.spark.sparkzk.zkclient.common.serializer.containerLaunchContext;

/**
 * Created by ubuntu2 on 9/24/17.
 */
public enum MyLocalResourceType {

    /**
     * Archive, which is automatically unarchived by the <code>NodeManager</code>.
     */
    ARCHIVE,

    /**
     * Regular file i.e. uninterpreted bytes.
     */
    FILE,

    /**
     * A hybrid between archive and file.  Only part of the file is unarchived,
     * and the original file is left in place, but in the same directory as the
     * unarchived part.  The part that is unarchived is determined by pattern
     *
     */
    PATTERN
}
