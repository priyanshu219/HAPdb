package db;

import java.io.File;

public class CleanUtil {
    public static void deleteDirectory(String directoryName) {
        File directory = new File(directoryName);
        for (String fileName : directory.list()) {
            new File(directory, fileName).delete();
        }
    }
}
