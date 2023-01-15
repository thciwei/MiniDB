package com.thciwei.minidb.client;

import com.thciwei.minidb.transport.Package;
import com.thciwei.minidb.transport.Packager;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc
 */
public class Client {
    private RoundTripper roundTripper;

    public Client(Packager packager) {
        this.roundTripper = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        //封装为数据包
        Package pkg = new Package(stat, null);
        Package resPkg = roundTripper.roundTrip(pkg);
        if (resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            roundTripper.close();
        } catch (Exception e) {

        }

    }


}
