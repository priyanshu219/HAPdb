package db.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class FileManager {
    private final File dbDirectory;
    private final int blocksize;
    private final boolean isNew;
    private final Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileManager(File dbDirectory, int blocksize) {
        this.dbDirectory = dbDirectory;
        this.blocksize = blocksize;
        isNew = !dbDirectory.exists();

        if (isNew) {
            dbDirectory.mkdirs();
        }

        for (String filename : dbDirectory.list()) {
            if (filename.startsWith("temp")) {
                new File(dbDirectory, filename).delete();
            }
        }
    }

    public synchronized void read(Block block, Page p) throws IOException {
        try {
            RandomAccessFile file = getFile(block.getFileName());
            file.seek((long) block.getBlockNumber() * blocksize);
            file.getChannel().read(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + block);
        }
    }

    public synchronized void write(Block block, Page page) {
        try {
            RandomAccessFile file = getFile(block.getFileName());
            file.seek((long) block.getBlockNumber() * blocksize);
            file.getChannel().write(page.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot write block " + block);
        }
    }

    public synchronized Block append(String filename) {
        int newblknum = length(filename);
        Block block = new Block(filename, newblknum);
        byte[] bytes = new byte[blocksize];
        try {
            RandomAccessFile file = getFile(block.getFileName());
            file.seek((long) block.getBlockNumber() * blocksize);
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
            return (int) (file.length() / blocksize);
        } catch (IOException e) {
            throw new RuntimeException("cannot access " + filename);
        }
    }

    public boolean isNew() {
        return isNew;
    }

    public int getBlocksize() {
        return blocksize;
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
