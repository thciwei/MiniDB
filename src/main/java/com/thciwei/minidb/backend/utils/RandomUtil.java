package com.thciwei.minidb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc
 */
public class RandomUtil {
    public static byte[] randomBytes(int length) {
        Random r = new SecureRandom();
        byte[] buffer = new byte[length];
        r.nextBytes(buffer);
        return buffer;
    }
}
