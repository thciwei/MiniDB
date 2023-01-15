package com.thciwei.minidb.client;

import java.util.Scanner;

/**
 * @author thciwei
 * @email qiaosong.wang@foxmail.com
 * @desc
 */
public class Shell {
    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        try {
            while (true) {
                System.out.print(":> ");
                String statStr = scanner.nextLine();
                if("exit".equals(statStr)||"quit".equals(statStr)){
                    break;
                }
                try {
                    byte[] res=client.execute(statStr.getBytes());
                    System.out.println(new String(res));
                }catch (Exception e){
                    System.out.println(e.getMessage());
                }
            }
        }finally {
            scanner.close();
            client.close();
        }
    }
}
