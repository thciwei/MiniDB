package top.wangqiaosong.minidb.transport;

/**
 * 将数据封装为数据包
 * [Flag][data]
 * 若 flag 为 0，表示发送的是数据，那么 data 即为这份数据本身；
 * 如果 flag 为 1，表示发送的是错误，data 是 err( Exception.getMessage()) 的错误提示信息。
 */
public class Package {
    byte[] data;
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
