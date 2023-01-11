package com.thciwei.minidb.backend.dm.dataItem;

import com.thciwei.minidb.backend.common.SubArray;
import com.thciwei.minidb.backend.dm.DataManager;
import com.thciwei.minidb.backend.dm.DataManagerImpl;
import com.thciwei.minidb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem {
    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dm;
    private long uid;
    private Page pg;

    DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte) 0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.lock();
    }

    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.lock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    /**
     * 写锁 方法
     */
    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    /**
     * 读锁方法
     */
    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return null;
    }

    @Override
    public long getUid() {
        return 0;
    }

    @Override
    public byte[] getOldRaw() {
        return new byte[0];
    }

    @Override
    public SubArray getRaw() {
        return null;
    }




}
