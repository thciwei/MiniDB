package com.thciwei.minidb.backend.tbm;

import com.thciwei.minidb.backend.dm.DataManager;
import com.thciwei.minidb.backend.parser.statement.*;
import com.thciwei.minidb.backend.utils.Parser;
import com.thciwei.minidb.backend.vm.VersionManager;
import com.thciwei.minidb.common.Error;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc
 */
public class TableManagerImpl implements TableManager {
    VersionManager vm;
    DataManager dm;
    private Booter booter;
    private Map<String, Table> tableCache;
    private Map<Long, List<Table>> xidTableCache;
    private Lock lock;

    public TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
    }

    private void loadTables() {
        long uid = firstTableUid();
        while (uid != 0) {
            Table tb = Table.loadTable(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }

    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead ? 1 : 0;
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }

    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }


    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }

    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table table : tableCache.values()) {
                sb.append(table.toString()).append("\n");
            }
            List<Table> t = xidTableCache.get(xid);
            if (t == null) {
                return "\n".getBytes();
            }
            for (Table table : t) {
                sb.append(table.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }

    }

    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if (tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            if (!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return ("create" + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TableNotFoundException;
        }
        table.insert(xid, insert);
        return "insert".getBytes();
    }

    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(xid, read).getBytes();
    }


    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(xid, update);
        return ("update" + count).getBytes();
    }

    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {

        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete" + count).getBytes();
    }
}
