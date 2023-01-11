package com.thciwei.minidb.backend.vm;

import com.thciwei.minidb.backend.dm.DataManager;
import com.thciwei.minidb.backend.tm.TransactionManager;

/**
 * 版本管理，向上层提供一些功能如下
 */
public interface VersionManager {
    byte[] read(long xid,long uid) throws Exception;
    long insert(long xid,byte[] data)throws Exception;
    boolean delete(long xid,long uid)throws Exception;


    //level是隔离级别
    long begin(int level);
    void commit(long xid)throws Exception;
    void abort(long xid);

    public static  VersionManager newVersionManager(TransactionManager tm, DataManager dm){
        return new VersionManagerImpl(tm,dm);
    }
}
