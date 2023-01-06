package com.thciwei.minidb;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 */

public class Test {
    @org.junit.Test
    public void hello() {
        int i = 1 << 13;
        System.out.println(i);
        int x = 1;
        byte y = 2;
        System.out.println(x * 13331 + y);
        System.out.println((short) ((short) 65535 & ((1L << 16) - 1)));
        System.out.println(32 >> 2);
        System.out.println(1L << 32);//2的32次方，000000....00001 左移100....0000，4294967296= 4GB=2的32B
        System.out.println(3L&4294967296L);
        System.out.println(3L&4294967295L);
    }


}
