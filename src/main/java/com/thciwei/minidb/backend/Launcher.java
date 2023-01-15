package com.thciwei.minidb.backend;

import com.thciwei.minidb.backend.dm.DataManager;
import com.thciwei.minidb.backend.server.Server;
import com.thciwei.minidb.backend.tbm.TableManager;
import com.thciwei.minidb.backend.tm.TransactionManager;
import com.thciwei.minidb.backend.tm.TransactionManagerImpl;
import com.thciwei.minidb.backend.utils.Panic;
import com.thciwei.minidb.backend.vm.VersionManager;
import com.thciwei.minidb.backend.vm.VersionManagerImpl;
import com.thciwei.minidb.common.Error;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc
 */
public class Launcher {
    public static final int port = 9999;
    public static final long DEFAULT_MEM = (1 << 20) * 64;
    public static final long KB = 1 << 10;

    public static final long MB = 1 << 20;

    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        DefaultParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("open")) {
          openDB(cmd.getOptionValue("open"),parseMem(cmd.getOptionValue("mem")));
        }
    }

    /**
     * 创建DB 开启一个事务
     */
    public static void createDB(String path) {
        TransactionManagerImpl tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFAULT_MEM, tm);
        VersionManagerImpl vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    private static long parseMem(String memStr) {
        if (memStr == null || "".equals(memStr)) {
            return DEFAULT_MEM;
        }
        if (memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length() - 2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length() - 2));
        switch (unit) {
            case "KB":
                return memNum * KB;
            case "MB":
                return memNum * MB;
            case "GB":
                return memNum * GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFAULT_MEM;

    }

}
