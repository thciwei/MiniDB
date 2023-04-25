package top.wangqiaosong.minidb.backend.parser.statement;

/**
 * 用于表管理和sql解析
 */
public class Create {
    public String tableName;
    //存储的字段名
    public String[] fieldName;
    //存储的字段类型
    public String[] fieldType;
    //存储的索引
    public String[] index;
}
