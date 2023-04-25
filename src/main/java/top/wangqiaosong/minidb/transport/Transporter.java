package top.wangqiaosong.minidb.transport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * 接收输入|输出流，以类似Scanner形式
 * 编码之后的信息会通过 Transporter 类，写入输出流发送出去。
 * 为了避免特殊字符造成问题，这里会将数据转成十六进制字符串（Hex String），并为信息末尾加上换行符。
 * 这样在发送和接收数据时，就可以很简单地使用 BufferedReader 和 Writer 来直接按行读写了。
 */
public class Transporter {
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        //BufferedReader相对于scanner更快，且支持同步，多线程场景下更应该使用
        //socket取数据 socket.getInputStream()
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    /**
     * 对数据16进制编码然后发送
     */
    public void send(byte[] data) throws Exception {
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    /**
     * 收到数据包解码
     */
    public byte[] receive() throws Exception {
        //按行读
        String line = reader.readLine();
        if (line == null) {
            close();
        }
        //解码
        return hexDecode(line);
    }

    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    private String hexEncode(byte[] buf) {
        return Hex.encodeHexString(buf, true) + "\n";
    }

    /**
     * 16进制解码
     */
    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }
}
