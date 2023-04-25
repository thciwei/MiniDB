package top.wangqiaosong.minidb.backend.vm;

import top.wangqiaosong.minidb.backend.dm.DataManager;
import top.wangqiaosong.minidb.backend.tm.TransactionManager;

public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    /**
     * level是隔离级别
     */
    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }

}
