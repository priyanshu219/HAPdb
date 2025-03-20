package db.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    private final ByteBuffer byteBuffer;
    public static final Charset CHARSET = StandardCharsets.US_ASCII;

    public Page(int blocksize) {
        this.byteBuffer = ByteBuffer.allocateDirect(blocksize);
    }

    public Page(byte[] bytes) {
        byteBuffer = ByteBuffer.wrap(bytes);
    }

    public int getInt(int offset) {
        return byteBuffer.getInt(offset);
    }

    public void setInt(int offset, int n) {
        byteBuffer.putInt(offset, n);
    }

    public byte[] getBytes(int offset) {
        byteBuffer.position(offset);
        int length = byteBuffer.getInt();
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        return bytes;
    }

    public void setBytes(int offset, byte[] bytes) {
        byteBuffer.position(offset);
        byteBuffer.putInt(bytes.length);
        byteBuffer.put(bytes);
    }

    public String getString(int offset) {
        byte[] b = getBytes(offset);
        return new String(b, CHARSET);
    }

    public void setString(int offset, String value) {
        byte[] bytes = value.getBytes(CHARSET);
        setBytes(offset, bytes);
    }

    public static int maxLength(int strlen) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strlen * (int) bytesPerChar);
    }

    protected ByteBuffer contents() {
        byteBuffer.position(0);
        return byteBuffer;
    }
}
