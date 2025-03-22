package db;

import db.buffer.BufferManager;
import db.file.FileManager;
import db.log.LogManager;
import db.server.HAPdb;
import db.transaction.Transaction;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;

public class FileConfig implements BeforeAllCallback, AfterAllCallback {
    private static HAPdb db;
    private String directoryName;

    public static HAPdb getDb() {
        return db;
    }

    public static FileManager fileManager() {
        return db.getFileManager();
    }

    public static LogManager logManager() {
        return db.getLogManager();
    }

    public static BufferManager bufferManager() {
        return db.getBufferManager();
    }

    public static Transaction newTransaction() {
        return new Transaction(fileManager(), logManager(), bufferManager());
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        TestConfig config = extensionContext.getRequiredTestClass().getAnnotation(TestConfig.class);

        directoryName = config.directoryName();
        int blockSize = config.blockSize();
        int totalBuffers = config.totalBuffers();

        db = new HAPdb(directoryName, blockSize, totalBuffers);
    }

    @Override
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void afterAll(ExtensionContext extensionContext) throws IOException {
        File directory = new File(directoryName);
        for (String fileName : directory.list()) {
            new File(directory, fileName).delete();
        }
        directory.delete();
    }
}
