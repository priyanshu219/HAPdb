package simpledb.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class FileMgr {
    private final File dbDirectory;
    private final int blocksize;
    private final boolean isNew;
    private final Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileMgr(File dbDirectory, int blocksize) {
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

    public synchronized void read(Block blk, Page p) throws IOException {
        try {
            RandomAccessFile f = getFile(blk.getFilename());
            f.seek((long) blk.getBlockNumber() * blocksize);
            f.getChannel().read(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk);
        }
    }

    public synchronized void write(Block blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.getFilename());
            f.seek((long) blk.getBlockNumber() * blocksize);
            f.getChannel().write(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot write block " + blk);
        }
    }

    public synchronized Block append(String filename) {
        int newblknum = length(filename);
        Block blk = new Block(filename, newblknum);
        byte[] b = new byte[blocksize];
        try {
            RandomAccessFile f = getFile(blk.getFilename());
            f.seek((long) blk.getBlockNumber() * blocksize);
            f.write(b);

            return blk;
        } catch (IOException e) {
            throw new RuntimeException("cannot append block " + blk);
        }
    }

    //This function return the block number to whom we are updating
    public int length(String filename) {
        try {
            RandomAccessFile f = getFile(filename);
            return (int) (f.length() / blocksize);
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
        RandomAccessFile f = openFiles.get(filename);
        if (null == f) {
            File dbTable = new File(dbDirectory, filename);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, f);
        }
        return f;
    }
}
