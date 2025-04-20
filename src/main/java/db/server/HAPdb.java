package db.server;

import db.buffer.BufferManager;
import db.file.FileManager;
import db.index.planner.IndexUpdatePlanner;
import db.log.LogManager;
import db.metadata.MetadataManager;
import db.planner.*;
import db.transaction.Transaction;

import java.io.File;

public class HAPdb {
    public static final String LOG_FILE = "hapdb.log";
    private static final int BLOCKSIZE = 800;
    private static final int TOTALBUFFERS = 8;
    private final FileManager fileManager;
    private final LogManager logManager;
    private final BufferManager bufferManager;
    private MetadataManager metadataManager;

    public HAPdb(String directoryName, int blockSize, int totalBuffers) {
        File dbDirectory = new File(directoryName);
        this.fileManager = new FileManager(dbDirectory, blockSize);
        this.logManager = new LogManager(fileManager, LOG_FILE);
        this.bufferManager = new BufferManager(fileManager, logManager, totalBuffers);
    }

    public HAPdb(String directoryName) {
        this(directoryName, BLOCKSIZE, TOTALBUFFERS);
        Transaction transaction = getNewTransaction();
        transaction.recover();

        boolean isNew = fileManager.isNew();
        this.metadataManager = new MetadataManager(isNew, transaction);
        transaction.commit();
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

    public Transaction getNewTransaction() {
        return new Transaction(fileManager, logManager, bufferManager);
    }

    public Planner getPlanner() {
        QueryPlanner queryPlanner = new BasicQueryPlanner(metadataManager);
//        UpdatePlanner updatePlanner = new BasicUpdatePlanner(metadataManager);
        UpdatePlanner updatePlanner = new IndexUpdatePlanner(metadataManager);
        return new Planner(queryPlanner, updatePlanner);

    }
}
