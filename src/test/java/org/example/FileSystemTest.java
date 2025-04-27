package org.example;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public class FileSystemTest {

    FileSystem fileSystem = new FileSystem();

    public FileSystemTest() throws NoSuchAlgorithmException {
    }

    @Test
    void shouldSaveAndRead() throws IOException {
        byte[] content = Files.readAllBytes(Paths.get("random_file.bin"));
        byte[] content2 = Files.readAllBytes(Paths.get("random_file_2.bin"));

        UUID contentUUID = UUID.randomUUID();
        UUID content2UUID = UUID.randomUUID();

        fileSystem.save(contentUUID, content);
        fileSystem.save(content2UUID, content2);

        byte[] readContent = fileSystem.read(contentUUID);
        byte[] readContent2 = fileSystem.read(content2UUID);

        Assertions.assertArrayEquals(content, readContent);
        Assertions.assertArrayEquals(content2, readContent2);

        fileSystem.delete(contentUUID);
        fileSystem.delete(content2UUID);

        Assertions.assertEquals(0, fileSystem.getTakenSectorsSize());

    }

}
