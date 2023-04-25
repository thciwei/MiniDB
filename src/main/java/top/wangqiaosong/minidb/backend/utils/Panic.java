package top.wangqiaosong.minidb.backend.utils;

/**
 * 异常处理类
 */
public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
