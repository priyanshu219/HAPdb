package db.file;

import db.FileConfig;
import db.TestConfig;
import db.server.HAPdb;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(FileConfig.class)
@TestConfig(directoryName = "file_test", blockSize = 400, totalBuffers = 8)
class FileManagerTest {

    @Test
    public void fileManagerTest() {
        FileManager fileManager = FileConfig.fileManager();

        Block block = new Block("testfile", 2);
        Page page1 = new Page(fileManager.getBlockSize());
        int position1 = 88;
        String randomeString = "someRandomString";
        page1.setString(position1, randomeString);
        int stringSize = Page.maxLength(randomeString.length());
        int position2 = position1 + stringSize;
        page1.setInt(position2, 2109);
        fileManager.write(block, page1);

        Page page2 = new Page(fileManager.getBlockSize());
        fileManager.read(block, page2);

        assert page2.getInt(position2) == 2109;
        assert page2.getString(position1).equals(randomeString);
    }
}