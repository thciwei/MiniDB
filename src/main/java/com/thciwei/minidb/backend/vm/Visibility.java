package com.thciwei.minidb.backend.vm;

import com.thciwei.minidb.backend.tm.TransactionManager;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc 事务可见性 与隔离级别
 */
public class Visibility {


    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if (t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }


    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if (t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }


    /**
     * 可重复读
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //该版本由事务A创建（xmin=xid）且未被删除（xmax=0）
        if (xmin == xid && xmax == 0) return true;
        // 事务A已经提交，
        if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if (xmax == 0) return true;
            if (xmax != xid) {
                if (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //未被删除
        if (xmin == xid && xmax == 0) return true;
        //如果该版本是由已提交的事务A创建
        if (tm.isCommitted(xmin)) {
            //且未被删除
            if (xmax == 0) return true;
            //或
            if (xmax != xid) {
                //由已经未提交的事务B创建
                if (!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }
}
