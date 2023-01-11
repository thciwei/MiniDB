package com.thciwei.minidb.backend.vm;

import com.google.common.primitives.Bytes;
import com.thciwei.minidb.backend.common.SubArray;
import com.thciwei.minidb.backend.dm.dataItem.DataItem;
import com.thciwei.minidb.backend.utils.Parser;

import java.util.Arrays;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc VM向上层抽象出entry
 * entry结构：
 * [XMIN] [XMAX] [data]
 * XMAX是我们规定的一个删除该版本的字段，!=NULL则为删除
 */
public class Entry {

    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN + 8;
    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;

    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        //使用dm层的read方法读取DataItem
        DataItem di = ((VersionManagerImpl) vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    public void release() {
        ((VersionManagerImpl) vm).releaseForCache(this);
    }

    public void remove() {
        dataItem.release();
    }

    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            //把raw数组拷贝到data数组，长度为data.length
            System.arraycopy(sa.raw, sa.start + OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMIN, sa.start + OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMAX, sa.start + OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    public void setXmax(long xid) {
        //修改操作前，变为脏页
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start + OF_XMAX, 8);
        } finally {
            //最后将修改操作写入日志
            dataItem.after(xid);
        }

    }

    public long getUid() {
        return uid;
    }


}
