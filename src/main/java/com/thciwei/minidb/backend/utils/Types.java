package com.thciwei.minidb.backend.utils;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc 转uid工具
 */
public class Types {
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long) pgno;
        long u1 = (long) pgno;
        return u0 << 32 | u1;
    }

}
