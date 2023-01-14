package com.thciwei.minidb.backend.tbm;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 */
public class Table {
    TableManager tbm;

}
