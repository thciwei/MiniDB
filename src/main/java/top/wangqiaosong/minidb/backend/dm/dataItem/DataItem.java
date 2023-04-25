package top.wangqiaosong.minidb.backend.dm.dataItem;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import top.wangqiaosong.minidb.backend.common.SubArray;
import top.wangqiaosong.minidb.backend.dm.DataManagerImpl;
import top.wangqiaosong.minidb.backend.dm.page.Page;
import top.wangqiaosong.minidb.backend.utils.Parser;
import top.wangqiaosong.minidb.backend.utils.Types;

/**
 * Dataitem 为DataEngine为上层模块提供的数据抽象
 * 上层模块需要根据地址， 向DataEngine请求对应的Dataitem
 * 然后通过Data方法， 取得DataItem实际内容
 * 下面是一些关于DataItem的协议.
 * 数据共享:
 *   利用d.Data()得到的数据, 是内存共享的.
 * 数据项修改协议:
 *   上层模块在对数据项进行任何修改之前, 都必须调用d.Before(), 如果想撤销修改, 则再调用
 *   d.UnBefore(). 修改完成后, 还必须调用d.After(xid).
 *   DM会保证对Dataitem的修改是原子性的.
 * 数据项释放协议:
 *   上层模块不用数据项时, 必须调用d.Release()来将其释放
 */
public interface DataItem {
    SubArray data();

    void before();

    void unBefore();

    void after(long xid);

    void release();

    void lock();

    void unlock();

    void rLock();

    void rUnLock();

    Page page();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();

    /**
     * 封装DataItem
     */
    public static byte[] wrapDataItemRaw(byte[] raw) {
        //设置合法位，作为假删除，删除则改变该字段
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    // 从页面的offset处解析出dataItem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], pg, uid, dm);
    }

    /**
     * 修改valid，代表删除
     */
    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
