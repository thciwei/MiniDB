package com.thciwei.minidb.backend.dm.pageCache;


import com.thciwei.minidb.backend.dm.page.Page;

public interface PageCache {
    //向左移位，其实就是8192,2的13次方B，数据页大小默认为8KB
    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);

    Page getPage(int pgno) throws Exception;

    void close();

    void release(Page page);

    void truncateByBgno(int maxPgno);

    int getPageNumber();

    void flushPage(Page pg);

}
