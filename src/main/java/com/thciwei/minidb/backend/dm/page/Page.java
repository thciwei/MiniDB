package com.thciwei.minidb.backend.dm.page;

/**
 * 数据页
 */
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();

}
