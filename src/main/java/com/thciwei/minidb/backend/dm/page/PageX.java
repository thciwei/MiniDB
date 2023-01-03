package com.thciwei.minidb.backend.dm.page;

import com.thciwei.minidb.backend.dm.pageCache.PageCache;
import com.thciwei.minidb.backend.utils.Parser;

import java.util.Arrays;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc 数据页管理->普通页
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节(short) 空闲位置开始偏移
 */
public class PageX {
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;

    // 向页面插入数据，将raw插入pg中，返回插入位置
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        //FSO=FreeSpaceOffset
        short offset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(), (short) (offset + raw.length));
        return offset;
    }

    /**
     * 普通页管理setFSO和getFSO
     */
    private static void setFSO(byte[] raw, short ofData) {
        //从0-1，长2
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 获取页面空闲的空间大小
     */
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int) getFSO(pg.getData());
    }

    /**
     * 数据库崩溃后重新打开时，恢复例程直接插入数据以及修改数据
     * 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short) (offset + raw.length));
        }
    }
    /**
     * 奔溃重开时恢复例程修改数据，将raw插入pg中的offset位置
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }

}
