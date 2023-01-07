package com.thciwei.minidb.backend.dm.pageIndex;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc
 */
public class PageInfo {
    public  int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
