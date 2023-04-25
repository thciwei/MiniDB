package top.wangqiaosong.minidb.backend.vm;

import java.util.HashMap;
import java.util.Map;

import top.wangqiaosong.minidb.backend.tm.TransactionManagerImpl;

/**
 * vm对一个事务的抽象
 * 应用快照表
 * level==0 代表事务已经提交，读已提交
 */
public class Transaction {
    public long xid;
    public int level;
    //事务快照存储的是活跃列表，活跃指正在执行的项目
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        //如果事务已经提交则不在快照中
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
