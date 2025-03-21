package db.log;

import db.CleanUtil;
import db.file.Page;
import db.server.HAPdb;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

class LogManagerTest {
    private static LogManager logManager;

    public static void main(String[] args) {
        HAPdb db = new HAPdb("logtest", 400, 8);
        logManager = db.getLogManager();
        createRecords(1, 35);
        printLogRecords("The log file now has below records");
        createRecords(36, 70);
        logManager.flush(65);
        printLogRecords("The log file now has below records");

        CleanUtil.deleteDirectory("logtest");
    }

    private static void printLogRecords(String msg) {
        System.out.println(msg);
        Iterator<byte[]> iterator = logManager.iterator();
        while (iterator.hasNext()) {
            byte[] record = iterator.next();
            Page page = new Page(record);
            String stringValue = page.getString(0);
            int numPosition = Page.maxLength(stringValue.length());
            int intValue = page.getInt(numPosition);
            System.out.println("[" + stringValue + ", " + intValue + "]");
        }
        System.out.println();
    }

    private static void createRecords(int start, int end) {
        System.out.println("Creating records: ");
        for (int i = start; i <= end; i++) {
            byte[] record = createLogRecord("record" + i, i + 100);
            int lsn = logManager.append(record);
            System.out.print(lsn + " ");
        }
        System.out.println();
    }

    private static byte[] createLogRecord(String stringValue, int intValue) {
        int position = Page.maxLength(stringValue.length());
        byte[] bytes = new byte[position + Integer.BYTES];
        Page page = new Page(bytes);
        page.setString(0, stringValue);
        page.setInt(position, intValue);
        return bytes;
    }
}