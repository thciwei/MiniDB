package top.wangqiaosong.minidb.backend.dm.page;

/**
 * page.实现了关于Page的逻辑和接口.
 * 其中需要注意的是两个协议
 * Page更新协议:
 * 		在对Page做任何的更新之前, 一定需要调用Dirty().
 * Page释放协议:
 * 		在对Page操作完之后, 一定要调用Release()释放掉该页.
 */
public interface Page {
    void lock();
    void unlock();

    /**
     * 释放缓存
     */
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
