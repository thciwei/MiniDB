package com.thciwei.minidb.backend.utils;

import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * 工具类，long和byte[]等转型
 */

public class Parser {
    /**
     * 缓冲区对象ByteBuffer转为长整型Long对象
     *
     * @param buffer
     * @return
     */
    public static long parseLong(byte[] buffer) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, 8);
        return byteBuffer.getLong();
    }

    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }
}
