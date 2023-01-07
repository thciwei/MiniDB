package com.thciwei.minidb.backend.dm;

import com.thciwei.minidb.backend.dm.dataItem.DataItem;
import com.thciwei.minidb.backend.dm.logger.Logger;
import com.thciwei.minidb.backend.dm.page.PageOne;
import com.thciwei.minidb.backend.dm.pageCache.PageCache;
import com.thciwei.minidb.backend.dm.pageCache.PageCacheImpl;
import com.thciwei.minidb.backend.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();

    /**
     * 数据管理，加载日志和缓存、事务管理器
     */
    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }
    public  static DataManager open(String path,long mem,TransactionManager tm){
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if(!dm.loadCheckPageOne()){
            Recover.recover(tm,lg,pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);
    }


}
