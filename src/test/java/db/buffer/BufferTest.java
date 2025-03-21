package db.buffer;

import db.file.Block;
import db.file.Page;
import db.server.HAPdb;

import static org.junit.jupiter.api.Assertions.*;

class BufferTest {
    public static void main(String[] args) {
        HAPdb db = new HAPdb("buffertest", 400, 3);
        BufferManager bufferManager = db.getBufferManager();

        Buffer buffer1 = bufferManager.pin(new Block("testfile", 1));
        Page page = buffer1.getContents();

    }
}