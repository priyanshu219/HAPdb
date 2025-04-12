package db.transaction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LogArchiver {
    //TODO: use filemanager
    public static void archiveLog(String logFilePath, String archiveFilePath) throws IOException {
        Path logPath = Paths.get(logFilePath);
        if (!Files.exists(logPath)) {
            return;
        }

        Files.createDirectories(Paths.get(archiveFilePath));
        String timeStamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        Path archivePath = Paths.get(archiveFilePath, "log_" + timeStamp + ".log");

        Files.move(logPath, archivePath, StandardCopyOption.REPLACE_EXISTING);

        Files.createFile(logPath);
    }
}
