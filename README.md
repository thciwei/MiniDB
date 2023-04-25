# minidb

minidb 是一个 Java 实现的简单的数据库，部分原理参照自 MySQL、PostgreSQL 和 SQLite。实现了以下功能：

- 数据的可靠性和数据恢复
- 两段锁协议（2PL）实现可串行化调度
- MVCC
- 两种事务隔离级别（读提交和可重复读）
- 死锁处理
- 简单的表和字段管理
- 简陋的 SQL 解析（因为懒得写词法分析和自动机，就弄得比较简陋）
- 基于 socket 的 server 和 client

```
create table student id int32,name string (index id)
insert into student values 10 liming
select * from student where id=10
运行方式 使用相对路径在win下很容易报错
mvn exec:java -Dexec.mainClass="top.wangqiaosong.minidb.backend.Launcher" -Dexec.args="-create D:/biyesheji/miniDB/tmp/mydb"
mvn exec:java -Dexec.mainClass="top.wangqiaosong.minidb.backend.Launcher" -Dexec.args="-open D:/biyesheji/miniDB/tmp/mydb"
mvn exec:java -Dexec.mainClass="top.wangqiaosong.minidb.client.Launcher"

```

## 运行方式

注意首先需要在 pom.xml 中调整编译版本，如果导入 IDE，请更改项目的编译版本以适应你的 JDK

首先执行以下命令编译源码：

```shell
mvn compile
```

接着执行以下命令以 /tmp/mydb 作为路径创建数据库：

win11一定要在git下编译和运行，命令行会出问题，但使用wsl开发畅通无阻

```shell
//win下
mvn exec:java -Dexec.mainClass="top.wangqiaosong.minidb.backend.Launcher" -Dexec.args="-create D:\biyesheji\miniDB\src\main\java\top\guoziyang\mydb"
//wsl下
mvn exec:java -Dexec.mainClass="top.wangqiaosong.minidb.backend.Launcher" -Dexec.args="-create /mnt/d/biyesheji/miniDB"


mvn exec:java -Dexec.mainClass="top.wangqiaosong.minidb.backend.Launcher" -Dexec.args="-create D:/biyesheji/miniDB/tmp/mydb"

mvn exec:java -Dexec.mainClass="top.wangqiaosong.minidb.backend.Launcher" -Dexec.args="-create /mnt/d/biyesheji/miniDB"
```

随后通过以下命令以默认参数启动数据库服务：

```shell
mvn exec:java -Dexec.mainClass="top.wangqiaosong.minidb.backend.Launcher" -Dexec.args="-open D:/biyesheji/miniDB/tmp/mydb"

mvn exec:java -Dexec.mainClass="top.wangqiaosong.minidb.backend.Launcher" -Dexec.args="-open /mnt/d/biyesheji/miniDB/tmp/mydb"
```

这时数据库服务就已经启动在本机的 9999 端口。重新启动一个终端，执行以下命令启动客户端连接数据库：

```shell
mvn exec:java -Dexec.mainClass="top.wangqiaosong.minidb.client.Launcher"
```

会启动一个交互式命令行，就可以在这里输入类 SQL 语法，回车会发送语句到服务，并输出执行的结果。

一个执行示例：

![](https://s3.bmp.ovh/imgs/2021/11/2749906870276904.png)
