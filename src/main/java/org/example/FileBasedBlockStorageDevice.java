package org.example;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileBasedBlockStorageDevice implements BlockStorageDevice {

    private static final int BLOCK_SIZE = 4096;
    private final Path path;
    private final int numberOfBlocks;

    public FileBasedBlockStorageDevice(Path path, int numberOfBlocks) throws IOException {
        if (numberOfBlocks <= 0) {
            throw new IllegalArgumentException("Number of blocks must be positive.");
        }
        this.path = path;
        this.numberOfBlocks = numberOfBlocks;

        long totalSizeBytes = (long) numberOfBlocks * BLOCK_SIZE;
        if (Files.exists(path)) {
            long size = Files.size(path);
            if (size != totalSizeBytes) {
                throw new RuntimeException("Size of blocks does not match.");
            }
        } else {
            try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
                if (raf.length() < totalSizeBytes) {
                    raf.setLength(totalSizeBytes);
                }
            }
        }
    }

    @Override
    public byte[] read(int blockNumber) throws FileNotFoundException {
        validateBlockNumber(blockNumber);
        long offset = (long) blockNumber * BLOCK_SIZE;
        byte[] buffer = new byte[BLOCK_SIZE];

        //TODO tu tu tu tu
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            raf.seek(offset);
            int bytesRead = raf.read(buffer);

            // If EOF is reached before reading a full block (e.g., file truncated),
            // fill the remainder with zeros. A fresh file pre-allocated should not hit this.
            if (bytesRead > 0 && bytesRead < BLOCK_SIZE) {
                Arrays.fill(buffer, bytesRead, BLOCK_SIZE, (byte) 0);
            } else if (bytesRead == -1) {
                // EOF reached immediately (shouldn't happen with pre-allocation)
                // Return a zeroed buffer consistent with reading an unwritten block.
                Arrays.fill(buffer, (byte) 0);
            }
        }
        return buffer;
    }

    @Override
    public void write(int blockNumber, byte[] data) {
        validateBlockNumber(blockNumber);
        if (data == null) {
            throw new IllegalArgumentException("Data to write cannot be null.");
        }
        if (data.length != BLOCK_SIZE) {
            throw new IllegalArgumentException("Data length (" + data.length
                    + ") must exactly match block size (" + BLOCK_SIZE + ")");
        }

        long offset = (long) blockNumber * BLOCK_SIZE;

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.seek(offset);
            raf.write(data);
        }
    }

    private void validateBlockNumber(int blockNumber) {
        if (blockNumber < 0 || blockNumber >= this.numberOfBlocks) {
            throw new IndexOutOfBoundsException("Block number " + blockNumber
                    + " is out of range [0, " + (this.numberOfBlocks - 1) + "]");
        }
    }
}
