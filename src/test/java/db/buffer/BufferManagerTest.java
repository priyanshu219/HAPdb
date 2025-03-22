package db.buffer;

import db.FileConfig;
import db.TestConfig;
import db.file.Block;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(FileConfig.class)
@TestConfig(directoryName = "buffer_manager_test", blockSize = 400, totalBuffers = 3)
class BufferManagerTest {

    @Test
    public void bufferManagerTest() {
        BufferManager bufferManager = FileConfig.bufferManager();
        String testFile = "test_file";

        Buffer[] buffers = new Buffer[6];
        buffers[0] = bufferManager.pin(new Block(testFile, 1));
        buffers[1] = bufferManager.pin(new Block(testFile, 2));
        buffers[2] = bufferManager.pin(new Block(testFile, 3));

        assert bufferManager.getTotalAvailable() == 0;

        bufferManager.unpin(buffers[0]);
        assert bufferManager.getTotalAvailable() == 1;

        bufferManager.pin(new Block(testFile, 1));
        bufferManager.pin(new Block(testFile, 2));

        assert bufferManager.getTotalAvailable() == 0;

        Assertions.assertThrows(BufferAbortException.class, () -> bufferManager.pin(new Block(testFile, 4)));

        bufferManager.unpin(buffers[0]);
        assert bufferManager.getTotalAvailable() == 1;

        bufferManager.pin(new Block(testFile, 4));
        assert bufferManager.getTotalAvailable() == 0;
    }
}