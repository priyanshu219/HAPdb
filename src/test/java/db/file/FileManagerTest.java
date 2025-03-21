package db.file;

import db.server.HAPdb;

class FileManagerTest {
    public static void main(String[] args) {
        HAPdb db = new HAPdb("filetest", 400, 8);
        FileManager fileManager = db.getFileManager();

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