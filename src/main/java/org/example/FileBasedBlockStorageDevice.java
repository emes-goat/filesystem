package org.example;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.*;

public class FileBasedBlockStorageDevice implements BlockStorageDevice {

    private static final int BLOCK_SIZE = 4096;
    private final Path path;
    private final int numberOfBlocks;

    private final byte[] readBuffer = new byte[BLOCK_SIZE];

    public FileBasedBlockStorageDevice(Path path, int numberOfBlocks) throws IOException {
        checkArgument(numberOfBlocks > 0);
        this.path = path;
        this.numberOfBlocks = numberOfBlocks;

        long totalSizeBytes = (long) numberOfBlocks * BLOCK_SIZE;
        if (Files.exists(path)) {
            long size = Files.size(path);
            Preconditions.checkState(size == totalSizeBytes, "Invalid block storage size");
        } else {
            try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
                if (raf.length() < totalSizeBytes) {
                    raf.setLength(totalSizeBytes);
                }
            }
        }
    }

    @Override
    public byte[] read(int blockNumber) throws IOException {
        validateBlockNumber(blockNumber);

        long offset = (long) blockNumber * BLOCK_SIZE;

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            raf.seek(offset);
            int bytesRead = raf.read(readBuffer);
            Preconditions.checkState(bytesRead == BLOCK_SIZE);
        }
        return readBuffer;
    }

    @Override
    public void write(int blockNumber, byte[] data) throws IOException {
        validateBlockNumber(blockNumber);
        checkArgument(data != null);
        checkArgument(data.length == BLOCK_SIZE);

        long offset = (long) blockNumber * BLOCK_SIZE;

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.seek(offset);
            raf.write(data);
        }
    }

    private void validateBlockNumber(int blockNumber) {
        checkArgument(blockNumber >= 0 && blockNumber < this.numberOfBlocks,
                "Block must be between 0 and " + (numberOfBlocks - 1));
    }
}
