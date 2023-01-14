package com.thciwei.minidb.backend.parser;

import com.thciwei.minidb.common.Error;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc 分词器
 * 切分为多个Token
 */
public class Tokenizer {
    /**
     * 语句转为的数组-增删改查等
     */
    private byte[] stat;
    private int pos;
    private String currentToken;

    private boolean flushToken;
    private Exception err;

    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }
    /**
     * 查看下一个token
     */
    public String peek() throws Exception {
        if(err != null) {
            throw err;
        }
        if(flushToken) {
            String token = null;
            try {
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    /**
     * 弹出当前token
     */
    public void pop() {
        flushToken = true;
    }


    public byte[] errStat() {
        byte[] res = new byte[stat.length+3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos);
        return res;
    }

    /**
     * 数组指针
     */
    private void popByte() {
        pos ++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }


    private Byte peekByte() {
        if(pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    private String next() throws Exception {
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    /**
     * 处理语句
     */
    private String nextMetaState() throws Exception {
        while(true) {
            //遍历字节数组，直至遍历完
            Byte b = peekByte();
            if(b == null) {
                return "";
            }
            //遇到回车/空格忽略，继续遍历
            if(!isBlank(b)) {
                break;
            }
            //pos++;
            popByte();
        }
        byte b = peekByte();
        //如果b是<>=*,这些符号，取出符号
        if(isSymbol(b)) {
            popByte();
            //返回一个切割后的字符串
            return new String(new byte[]{b});
            //如果b是单引号或者双引号，找到单引号双引号包起来的内容 特殊表示方法
        } else if(b == '"' || b == '\'') {
            return nextQuoteState();
            //如果b是数字或字母，找到完整的数字或字符串
        } else if(isAlphaBeta(b) || isDigit(b)) {
            return nextTokenState();
        } else {
            err = Error.InvalidCommandException;
            throw err;
        }
    }

    /**
     * 提取英文字母
     */
    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            //跳过非字母数字
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                if(b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }

    /**
     * 判断数字
     */
    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    /**
     * 判断字母
     */
    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    /**
     * 提取引号内字段
     */
    private String nextQuoteState() throws Exception {
        byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            if(b == quote) {
                popByte();
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }

    /**
     * 判断大于小于等于号
     */
    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')');
    }

    /**
     * 判断空格
     */
    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
