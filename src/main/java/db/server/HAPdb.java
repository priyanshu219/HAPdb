package db.server;

import db.buffer.BufferManager;
import db.file.FileManager;
import db.log.LogManager;

import java.io.File;

public class HAPdb {
    public static final String LOG_FILE = "hapdb.log";
    private final FileManager fileManager;
    private final LogManager logManager;
    private final BufferManager bufferManager;

    public HAPdb(String directoryName, int blockSize, int totalBuffers) {
        File dbDirectory = new File(directoryName);
        this.fileManager = new FileManager(dbDirectory, blockSize);
        this.logManager = new LogManager(fileManager, LOG_FILE);
        this.bufferManager = new BufferManager(fileManager, logManager, totalBuffers);
    }

    public BufferManager getBufferManager() {
        return bufferManager;
    }

    public LogManager getLogManager() {
        return logManager;
    }

    public FileManager getFileManager() {
        return fileManager;
    }

}
