package com.thciwei.minidb.backend.parser;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc
 */
public class ParserTest {

    @Test
    public void tokenizer() throws Exception {
        String ss = " insert into student values 5 \"thciwei\" 22";
        byte[] b = ss.getBytes(StandardCharsets.UTF_8);
        Tokenizer tokenizer = new Tokenizer(b);
        for (int i = 0; i < b.length; i++) {
            String token = tokenizer.peek();
            System.out.println(token);
            tokenizer.pop();
        }
    }


}
