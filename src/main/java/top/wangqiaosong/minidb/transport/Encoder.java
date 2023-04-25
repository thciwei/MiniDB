package top.wangqiaosong.minidb.transport;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import top.wangqiaosong.minidb.common.Error;

public class Encoder {

    public byte[] encode(Package pkg) {
        if (pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if (err.getMessage() != null) {
                msg = err.getMessage();
            }
            //1表示后面为数据
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            //flag=0表示数据包携带的都是错误信息
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }


    public Package decode(byte[] data) throws Exception {
        if (data.length < 1) {
            throw Error.InvalidPkgDataException;
        }

        // 若 flag 为 0，表示发送的是数据，那么 data 即为这份数据本身；
        // 如果 flag 为 1，表示发送的是错误，data 是 err
        if (data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if (data[0] == 1) {
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            throw Error.InvalidPkgDataException;
        }
    }

}
