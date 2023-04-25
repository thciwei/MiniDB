package top.wangqiaosong.minidb.backend.common;
/**
 * @desc Java的数组是对象形式存储，而非指针指向一片连续的内存空间，
 * 规定数组可使用范围，简单的模拟其他语言共用同一片内存的方式
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
