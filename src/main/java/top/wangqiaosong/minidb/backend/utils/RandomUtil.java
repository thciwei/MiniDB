package top.wangqiaosong.minidb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * 随机字节生成器且线程安全
 */
public class RandomUtil {
    /**
     * 随机字节
     */
    public static byte[] randomBytes(int length) {
        Random r = new SecureRandom();
        byte[] buf = new byte[length];
        //随机字节生成器 如0747a9ad467d4dc29ce70344
        r.nextBytes(buf);
        return buf;
    }
}
