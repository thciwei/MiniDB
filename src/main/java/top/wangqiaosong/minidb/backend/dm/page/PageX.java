package top.wangqiaosong.minidb.backend.dm.page;

import java.util.Arrays;

import top.wangqiaosong.minidb.backend.dm.pageCache.PageCache;
import top.wangqiaosong.minidb.backend.utils.Parser;

/**
 * PageX管理普通页
 * 普通页结构
 * [Free  Space   Offset] [Data]
 * Free Space Offset: 2字节 空闲位置开始偏移
 * [Free Space Offset] 表示空闲空间的位置指针.
 * 空闲空间指存放数据的地方
 * Free Space Offset实际相当于一个指针，2字节的数字代表位置，表示从哪里开始存数据
 */
public class PageX {
    //raw代表 mysql中传输的字节数据
    private static final short OF_FREE = 0;
    // 一个普通页面以一个 2 字节无符号数起始，表示这一页的空闲位置的偏移。剩下的部分都是实际存储的data数据。
    private static final short OF_DATA = 2;
    /**
     * 空闲空间=页大小-初始占的2字节
     */
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        //普通页设置数据空间，pageX是普通页
        setFSO(raw, OF_DATA);
        return raw;
    }

    //插入数据 raw是插入数据，ofData表示要复制的数组，raw表示复制到的目的数组
    //srcPos指源数组要复制的起始位置，destPos指目的数组放置的起始位置，OF_DATA为复制的长度
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    // 获取pg的FSO
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    /**
     * raw代表数据库传递的字节数据
     * 拿setFSO放入的字节数据
     */
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    // 将raw插入pg中，返回插入位置
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(), (short) (offset + raw.length));
        return offset;
    }

    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int) getFSO(pg.getData());
    }

    // 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset(崩溃后恢复数据阶段)
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        //插入数据时页变为脏页
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short) (offset + raw.length));
        }
    }

    // 将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
