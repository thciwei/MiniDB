package com.thciwei.minidb.backend.dm;

import com.thciwei.minidb.backend.common.AbstractCache;
import com.thciwei.minidb.backend.dm.dataItem.DataItem;
import com.thciwei.minidb.backend.dm.dataItem.DataItemImpl;
import com.thciwei.minidb.backend.dm.logger.Logger;
import com.thciwei.minidb.backend.dm.page.Page;
import com.thciwei.minidb.backend.dm.page.PageOne;
import com.thciwei.minidb.backend.dm.page.PageX;
import com.thciwei.minidb.backend.dm.pageCache.PageCache;
import com.thciwei.minidb.backend.dm.pageIndex.PageIndex;
import com.thciwei.minidb.backend.dm.pageIndex.PageInfo;
import com.thciwei.minidb.backend.tm.TransactionManager;
import com.thciwei.minidb.backend.utils.Panic;
import com.thciwei.minidb.backend.utils.Types;
import com.thciwei.minidb.common.Error;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {
    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.UpdateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int) (uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    /**
     * 创建并初始化PageOne
     */
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);

    }

    /**
     * DataManager 被创建时，需要获取所有页面并填充 PageIndex：
     */
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for (int i = 2; i <= pageNumber; i++) {
            Page pg = null;

            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }

    /**
     * 校验pageOne
     */
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if (!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }
        PageInfo pi = null;
        for (int i = 0; i < 5; i++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPage = pc.newPage(PageX.initRaw());
                pIndex.add(newPage, PageX.MAX_FREE_SPACE);
            }
        }
        if (pi == null) {
            throw Error.DatabaseBusyException;
        }
        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);
            short offset = PageX.insert(pg, raw);
            pg.release();
            return Types.addressToUid(pi.pgno, offset);
        } finally {
            if (pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }

    }

    @Override
    public void close() {
        super.close();
        logger.close();
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }


}
