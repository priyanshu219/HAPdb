package db.buffer;

import db.FileConfig;
import db.TestConfig;
import db.file.Block;
import db.file.Page;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(FileConfig.class)
@TestConfig(directoryName = "buffer_test", blockSize = 400, totalBuffers = 3)
class BufferTest {

    @Test
    public void bufferTest() {
        BufferManager bufferManager = FileConfig.bufferManager();

        Buffer buffer1 = bufferManager.pin(new Block("testfile", 1));
        Page page = buffer1.getContents();

        int intValue = page.getInt(80);
        page.setInt(80, intValue + 1);
        buffer1.setModified(1, 0);

        bufferManager.unpin(buffer1);

        // one of them replace buffer1 page, and flushed the changes
        Buffer buffer = bufferManager.pin(new Block("testfile", 2));
        bufferManager.pin(new Block("testfile", 3));
        bufferManager.pin(new Block("testfile", 4));


        assert buffer1.getBlock().blockNumber() != 1;

        bufferManager.unpin(buffer);
        Buffer buffer2 = bufferManager.pin(new Block("testfile", 1));
        assert buffer2.getContents().getInt(80) == (intValue + 1);
    }
}