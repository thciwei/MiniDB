package com.thciwei.minidb.backend.dm;

import com.thciwei.minidb.backend.dm.logger.Logger;
import com.thciwei.minidb.backend.dm.page.Page;
import com.thciwei.minidb.backend.dm.page.PageX;
import com.thciwei.minidb.backend.dm.pageCache.PageCache;
import com.thciwei.minidb.backend.tm.TransactionManager;
import com.thciwei.minidb.backend.utils.Panic;
import com.thciwei.minidb.backend.utils.Parser;

import java.util.*;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc redo log 重做日志，提交事务
 * undo log 撤销，但是 miniDB没有真删除，多一个字段将这些事务设置为非法，逻辑删除
 */
public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    /**
     * 写入日志类
     */
    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    /**
     * 更新日志类
     */
    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger logger, PageCache pc) {
        System.out.println("Recovering...");

        logger.rewind();
        int maxPgno = 0;
        while (true) {
            byte[] log = logger.next();
            if (log == null) break;
            int pgno;
            if (isInsertLog(log)) {
                InsertLogInfo logInfo = parseInsertLog(log);
                pgno = logInfo.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if (pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if (maxPgno == 0) {
            maxPgno = 1;
        }

        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTransactions(tm, logger, pc);
        System.out.println("Redo Transactions Over.");

        undoTransactions(tm, logger, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");

    }

    /**
     * 重做所有已完成事务
     */
    private static void redoTransactions(TransactionManager tm, Logger logger, PageCache pc) {
        logger.rewind();
        while (true) {
            byte[] log = logger.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo logInfo = parseInsertLog(log);
                long xid = logInfo.xid;
                if (!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if (!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }

    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo logInfo = new UpdateLogInfo();
        //xid 8字节 访问1-9
        logInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        logInfo.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        logInfo.pgno = (int) (uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        logInfo.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        logInfo.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return logInfo;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        if (flag == REDO) {
            UpdateLogInfo logInfo = parseUpdateLog(log);
            pgno = logInfo.pgno;
            offset = logInfo.offset;
            //new新的
            raw = logInfo.newRaw;
        } else {
            UpdateLogInfo logInfo = parseUpdateLog(log);
            pgno = logInfo.pgno;
            offset = logInfo.offset;
            //old不变
            raw = logInfo.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }

    }


    /**
     * 撤销所有未完成事务 transaction
     */
    private static void undoTransactions(TransactionManager tm, Logger logger, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        logger.rewind();
        while (true) {
            byte[] log = logger.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo logInfo = parseInsertLog(log);
                long xid = logInfo.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        //如果没有这个xid那就存入一键值对
                        logCache.put(xid, new ArrayList<>());
                    }
                    //如果有这个xid直接在value追加log
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }
    }

    /**
     * updateLog格式  [LogType] [XID] [UID] [OldRaw] [NewRaw]
     */
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    /**
     * insertLog格式 [LogType] [XID] [Pgno] [Offset] [Raw] raw代表二进制数据
     */
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return null;
    }


    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo insertLog = parseInsertLog(log);
        Page page = null;
        try {
            page = pc.getPage(insertLog.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }

        if (flag == UNDO) {

        }
        page.release();

    }

    /**
     * 校验是否插入日志
     */
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }


}
