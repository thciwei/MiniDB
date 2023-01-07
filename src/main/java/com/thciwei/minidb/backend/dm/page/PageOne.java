package com.thciwei.minidb.backend.dm.page;

import com.thciwei.minidb.backend.dm.pageCache.PageCache;
import com.thciwei.minidb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc 数据页管理->第一页
 * 合法性检查 ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 * <p>
 * 这里说明以下 raw代表数据库中传递的字节数据，遵循 MySQL命名
 */
public class PageOne {
    /**
     * 第一页起始点
     */
    private static final int OF_VC = 100;
    /**
     * 合法长度
     */
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    /**
     * 启动时设置初始字节
     */
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    /**
     * 关闭时第一段字节拷贝到108 ~ 115
     */
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    public static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    /**
     * 校验字节，启动时判断上次是否正常关闭
     * 否则开始数据恢复
     */
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    /**
     * 判断第一段8字节和第二段是否相同
     */
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC), Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC));
    }


}
