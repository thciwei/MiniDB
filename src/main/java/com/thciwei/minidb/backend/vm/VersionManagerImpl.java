package com.thciwei.minidb.backend.vm;

import com.thciwei.minidb.backend.common.AbstractCache;
import com.thciwei.minidb.backend.dm.DataManager;
import com.thciwei.minidb.backend.tm.TransactionManager;
import com.thciwei.minidb.backend.tm.TransactionManagerImpl;
import com.thciwei.minidb.backend.utils.Panic;
import com.thciwei.minidb.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc vm实现类设计为Entry的缓存
 *
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {
    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        return new byte[0];
    }


    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if (t.err != null) {
            throw t.err;
        }
        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.unlock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry= super.get(uid);
        } catch (Exception e) {
            if(e==Error.NullEntryException){
                return false;
            }else {
                throw e;
            }
        }

        try {
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                l = lt.add(xid, uid);
            } catch(Exception e) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            if(l != null) {
                l.lock();
                l.unlock();
            }

            if(entry.getXmax() == xid) {
                return false;
            }

            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }
    }

    /**
     * 开启一个事务，并初始化事务的结构，将其存放在 activeTransaction 中，用于检查和快照使用
     */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 提交一个事务，主要就是 free 掉相关的结构，并且释放持有的锁，并修改 TM 状态
     */
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        try {
            if (t.err != null) {
                throw t.err;
            }
        } catch (NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }
        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();
        lt.remove(xid);
        tm.commit(xid);
    }

    /**
     * 手动中止
     *
     * @param xid
     */
    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    /**
     * 自动中止并回滚
     */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if (!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();
        if (t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }

    /**
     * entry的缓存方法
     */
    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if (entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
}
