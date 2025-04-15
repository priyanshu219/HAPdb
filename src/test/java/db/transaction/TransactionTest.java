package db.transaction;

import db.FileConfig;
import db.TestConfig;
import db.buffer.BufferManager;
import db.file.FileManager;
import db.log.LogManager;
import db.server.HAPdb;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(FileConfig.class)
@TestConfig(directoryName = "transaction_test", blockSize = 400, totalBuffers = 8)
class TransactionTest {
    private final HAPdb db = FileConfig.getDb();
    private final FileManager fileManager = db.getFileManager();
    private final LogManager logManager = db.getLogManager();
    private final BufferManager bufferManager = db.getBufferManager();


    @Test
    void testQuiescentCheckpoint() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(8);

        CountDownLatch txn1Started = new CountDownLatch(1);
        CountDownLatch txn2Started = new CountDownLatch(1);
        CountDownLatch txnFinishedSignal = new CountDownLatch(1);

        Runnable txn1 = () -> {
            Transaction transaction = new Transaction(fileManager, logManager, bufferManager);
            txn1Started.countDown();
            try {
                txnFinishedSignal.await();
                transaction.commit();
            } catch (InterruptedException ignored) {
            }
        };

        Runnable txn2 = () -> {
            Transaction transaction = new Transaction(fileManager, logManager, bufferManager);
            txn2Started.countDown();
            try {
                txnFinishedSignal.await();
                transaction.commit();
            } catch (InterruptedException ignored) {
            }
        };

        executor.submit(txn1);
        executor.submit(txn2);

        txn1Started.await(1, TimeUnit.SECONDS);
        txn2Started.await(1, TimeUnit.SECONDS);

        Transaction transaction = new Transaction(fileManager, logManager, bufferManager);
        transaction.runQuiescentCheckpoint();
        transaction.commit();

        Future<Boolean> blockedTxn = executor.submit(() -> {
            new Transaction(fileManager, logManager, bufferManager);
            return true;
        });

        Thread.sleep(1000);
        assertFalse(blockedTxn.isDone());

        txnFinishedSignal.countDown();

        assertTrue(blockedTxn.get(5, TimeUnit.SECONDS), "New txn should start after checkpoint");
    }
}