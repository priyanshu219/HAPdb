package db.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    public static final Charset CHARSET = StandardCharsets.US_ASCII;
    private final ByteBuffer byteBuffer;

    //For creating block data storage
    public Page(int blockSize) {
        this.byteBuffer = ByteBuffer.allocateDirect(blockSize);
    }

    //For creating record pages
    public Page(byte[] bytes) {
        byteBuffer = ByteBuffer.wrap(bytes);
    }

    public static int maxLength(int strlen) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strlen * (int) bytesPerChar);
    }

    public int getInt(int offset) {
        return byteBuffer.getInt(offset);
    }

    public void setInt(int offset, int n) {
        if (offset + Integer.BYTES > byteBuffer.capacity()) {
            throw new PageFormatException("Attempt to write beyond page boundary");
        }
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
        if (offset + Integer.BYTES + bytes.length > byteBuffer.capacity()) {
            throw new PageFormatException("Attempt to write beyond page boundary");
        }
        byteBuffer.position(offset);
        byteBuffer.putInt(bytes.length);
        byteBuffer.put(bytes);
    }

    public String getString(int offset) {
        byte[] bytes = getBytes(offset);
        return new String(bytes, CHARSET);
    }

    public void setString(int offset, String value) {
        byte[] bytes = value.getBytes(CHARSET);
        setBytes(offset, bytes);
    }

    protected ByteBuffer contents() {
        byteBuffer.position(0);
        return byteBuffer;
    }
}
