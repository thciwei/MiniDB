package top.wangqiaosong.minidb.backend.dm.pageIndex;

public class PageInfo {
    //页码,mybatis一般这样表示
    /**
     * 页号
     */
    public int pgno;
    /**
     * 空闲空间即数据空间
     */
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
