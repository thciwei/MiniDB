package com.thciwei.minidb.backend.parser.statement;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc
 */
public class Where {
    public SingleExpression singleExp1;
    /**
     * 大于小于等于
     */
    public String logicOp;
    public SingleExpression singleExp2;
}
