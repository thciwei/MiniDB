package top.wangqiaosong.minidb.backend.utils;

/**
 * @desc 转uid工具
 * 将索引拿到的页码和对应普通页的位置(偏移量)转换为一个uid
 * uid首先代表一个数据页，事务操作关联数据页，xid关联uid
 */
public class Types {
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long)pgno;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}
