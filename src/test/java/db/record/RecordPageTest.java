package db.record;

import db.FileConfig;
import db.TestConfig;
import db.file.Block;
import db.transaction.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(FileConfig.class)
@TestConfig(directoryName = "record_page_test", blockSize = 400, totalBuffers = 3)
class RecordPageTest {

    @Test
    public void recordPageTest() {
        Transaction transaction = FileConfig.newTransaction();

        Schema schema = new Schema();
        schema.addIntField("FieldA");
        schema.addStringField("FieldB", 9);
        Layout layout = new Layout(schema);

        Block block = transaction.append("test_file");
        transaction.pin(block);

        RecordPage recordPage = new RecordPage(transaction, block, layout);
        recordPage.format();

        // to make sure that all the slots empty
        assert recordPage.nextAfter(-1) == -1;

        int slot = recordPage.insertAfter(-1);
        while (slot >= 0) {
            int fieldAValue = (int) Math.round(Math.random() * 50);
            recordPage.setInt(slot, "FieldA", fieldAValue);
            recordPage.setString(slot, "FieldB", "record" + fieldAValue);
            slot = recordPage.insertAfter(slot);
        }

        // no empty record slot
        assert recordPage.insertAfter(-1) == -1;

        slot = recordPage.nextAfter(-1);
        while (slot >= 0) {
            int fieldAValue = recordPage.getInt(slot, "FieldA");
            if (fieldAValue <= 20) {
                recordPage.delete(slot);
            }
            slot = recordPage.nextAfter(slot);
        }

        slot = recordPage.nextAfter(-1);
        while (slot >= 0) {
            int fieldAValue = recordPage.getInt(slot, "FieldA");

            // all used slots have int value greater than 20.
            assert fieldAValue > 20;

            slot = recordPage.nextAfter(slot);
        }

        transaction.unpin(block);
        transaction.commit();
    }
}