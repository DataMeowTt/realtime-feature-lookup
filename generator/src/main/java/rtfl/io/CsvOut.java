package rtfl.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class CsvOut {
    private CsvOut() {}

    public static void append(Path file, String header, String row) throws IOException {
        Files.createDirectories(file.toAbsolutePath().getParent());
        boolean exists = Files.exists(file);
        String text = (exists ? "" : header + "\n") + row + "\n";
        Files.writeString(file, text, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
