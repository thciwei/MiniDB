package top.wangqiaosong.minidb.backend.dm;

import top.wangqiaosong.minidb.backend.dm.dataItem.DataItem;
import top.wangqiaosong.minidb.backend.dm.logger.Logger;
import top.wangqiaosong.minidb.backend.dm.page.PageOne;
import top.wangqiaosong.minidb.backend.dm.pageCache.PageCache;
import top.wangqiaosong.minidb.backend.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    /**
     * 创建数据管理器，在此项目中一切接口的实现类才是主体
     * 从0创建，初始化页面即可
     */
    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    /**
     * 已有文件上创建，先启动检查，判断是否执行恢复流程，并重新在第一页生成字节
     */
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        //构建索引
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        //写入磁盘
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
