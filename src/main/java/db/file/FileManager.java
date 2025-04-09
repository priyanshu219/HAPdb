package db.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class FileManager {
    private final File dbDirectory;
    private final int blockSize;
    private final boolean isNew;
    private final Map<String, RandomAccessFile> openFiles;

    private int blockWritten;
    private int blockRead;

    public FileManager(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        this.openFiles = new HashMap<>();

        isNew = !dbDirectory.exists();
        blockWritten = 0;
        blockRead = 0;

        if (isNew) {
            dbDirectory.mkdirs();
        }

        for (String filename : Objects.requireNonNull(dbDirectory.list())) {
            if (filename.startsWith("temp")) {
                new File(dbDirectory, filename).delete();
            }
        }
    }

    public synchronized void read(Block block, Page page) {
        try {
            RandomAccessFile file = getFile(block.fileName());
            file.seek((long) block.blockNumber() * blockSize);
            file.getChannel().read(page.contents());

            blockRead++;
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + block);
        }
    }

    public synchronized void write(Block block, Page page) {
        try {
            RandomAccessFile file = getFile(block.fileName());
            file.seek((long) block.blockNumber() * blockSize);
            file.getChannel().write(page.contents());

            blockWritten++;
        } catch (IOException e) {
            throw new RuntimeException("cannot write block " + block);
        }
    }

    public synchronized Block append(String fileName) {
        int newblknum = length(fileName);
        Block block = new Block(fileName, newblknum);
        byte[] bytes = new byte[blockSize];
        try {
            RandomAccessFile file = getFile(block.fileName());
            file.seek((long) block.blockNumber() * blockSize);
            file.write(bytes);

            return block;
        } catch (IOException e) {
            throw new RuntimeException("cannot append block " + block);
        }
    }

    //This function return the block number to whom we are updating
    public int length(String filename) {
        try {
            RandomAccessFile file = getFile(filename);
            return (int) (file.length() / blockSize);
        } catch (IOException e) {
            throw new RuntimeException("cannot access " + filename);
        }
    }

    public boolean isNew() {
        return isNew;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public FileStatistics fileStatistics() {
        return new FileStatistics(blockWritten, blockRead);
    }

    private RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile file = openFiles.get(filename);
        if (null == file) {
            File dbTable = new File(dbDirectory, filename);
            file = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, file);
        }
        return file;
    }
}
