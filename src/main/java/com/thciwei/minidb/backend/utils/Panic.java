package com.thciwei.minidb.backend.utils;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * 异常处理,直接退出
 */
public class Panic {
    public static void panic(Exception e) {
        e.printStackTrace();
        System.exit(1);
    }
}
