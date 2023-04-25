package top.wangqiaosong.minidb.client;

import top.wangqiaosong.minidb.transport.Package;
import top.wangqiaosong.minidb.transport.Packager;

/**
 * 完成单次收发动作
 */
public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
