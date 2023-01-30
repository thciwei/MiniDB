# MiniDB
MiniDB是一个 Java 实现的简单数据库，部分原理参照自 MySQL、PostgreSQL 和 SQLite。实现了以下功能：

- 数据的可靠性和数据恢复
- 两段锁协议（2PL）实现可串行化调度
- MVCC
- 两种事务隔离级别（读提交和可重复读）
- 死锁处理
- 简单的表和字段管理
- 简陋的 SQL 解析
- 基于 socket 的 server 和 client
## 数据库架构
* TM---->事务管理器
* DM---->数据管理器
* VM---->版本管理器
* TBM--->表管理器
* IM---->索引管理器

![](https://gulimall-wqs.oss-cn-beijing.aliyuncs.com/typora/202301010933662.jpg)