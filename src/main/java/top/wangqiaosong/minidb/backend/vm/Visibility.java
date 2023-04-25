package top.wangqiaosong.minidb.backend.vm;

import top.wangqiaosong.minidb.backend.tm.TransactionManager;

public class Visibility {

    /**
     * 取出要修改的数据 X 的最新提交版本，并检查该最新版本的创建者对当前事务是否可见
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if (t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    /**
     * 事务可见性
     */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if (t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
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

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //该版本由事务A创建（xmin=xid）且未被删除（xmax=0）
        if (xmin == xid && xmax == 0) return true;
        // 由已经提交的事务B创建 且这个事务小于事务A且不在快照中(该事务在事务A开始前已经提交)
        if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if (xmax == 0) return true;
            if (xmax != xid) {
                // 该事务在事务A开始前未提交 或xmax>xid 这个事务在Ti开始之后才开始
                if (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
