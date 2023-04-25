package top.wangqiaosong.minidb.backend;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import top.wangqiaosong.minidb.backend.dm.DataManager;
import top.wangqiaosong.minidb.backend.server.Server;
import top.wangqiaosong.minidb.backend.tbm.TableManager;
import top.wangqiaosong.minidb.backend.tm.TransactionManager;
import top.wangqiaosong.minidb.backend.utils.Panic;
import top.wangqiaosong.minidb.backend.vm.VersionManager;
import top.wangqiaosong.minidb.backend.vm.VersionManagerImpl;
import top.wangqiaosong.minidb.common.Error;

/**
 * 服务器的启动入口。这个类解析了命令行参数。很重要的参数就是 -open 或者 -create。
 * Launcher 根据两个参数，来决定是创建数据库文件，还是启动一个已有的数据库。
 */
public class Launcher {

    public static final int port = 9999;
    //分配内存大小 memory 64MB mysql默认也是
    public static final long DEFALUT_MEM = (1 << 20) * 64;
    //2的10次方
    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        //使用apache的cli类，命令行参数
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if (cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    /**
     * Launcher 根据两个参数，来决定是创建数据库文件，
     * 创建DB 开启一个事务
     */
    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    /**
     * 还是启动一个已有的数据库。
     * 根据输入的文件地址和人工指定的内存大小启动数据库和文件
     */
    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    private static long parseMem(String memStr) {
        if (memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if (memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        //相当于切割掉字符串的KB/MB/GB字段，只剩数字
        String unit = memStr.substring(memStr.length() - 2);
        //切割 -mem xxMB的字符串，拿到内存大小参数xx
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
        return DEFALUT_MEM;
    }
}
