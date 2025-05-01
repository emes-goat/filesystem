package org.example;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.ImmutableIntArray;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class FileSystem {

    private final Integer DISK_SIZE = 5 * 1024 * 1024;
    private final Integer SECTOR_COUNT = DISK_SIZE / Constants.BLOCK_SIZE;

    private final Map<Integer, byte[]> dataBlocksHashes = new HashMap<>();
    private final BitSet dataBlockAvailability = new BitSet(SECTOR_COUNT);
    private final Map<UUID, Inode> files = new HashMap<>();

    private final MessageDigest digest = MessageDigest.getInstance("SHA-256");

    private final BlockStorageDevice storage = new FileBasedBlockStorageDevice(
            Paths.get("disk.bin"), 100);

    public FileSystem() throws NoSuchAlgorithmException, IOException {
    }

    public void save(UUID path, byte[] content) throws IOException {
        int requiredSectors = (int) Math.ceil((double) content.length / (double) Constants.BLOCK_SIZE);

        List<List<Byte>> chunkedContent = Lists.partition(Bytes.asList(content), Constants.BLOCK_SIZE);
        ImmutableIntArray assignedBlocks = findAvailableSectors(requiredSectors);
        Preconditions.checkState(chunkedContent.size() == assignedBlocks.length());

        for (int i = 0; i < chunkedContent.size(); i++) {
            byte[] sectorBytes = Bytes.toArray(chunkedContent.get(i));
            if (sectorBytes.length != Constants.BLOCK_SIZE) {
                sectorBytes = Arrays.copyOf(sectorBytes, Constants.BLOCK_SIZE);
            }
            byte[] hashSha256 = calculateSha256(sectorBytes);
            storage.write(assignedBlocks.get(i), sectorBytes);
            dataBlocksHashes.put(assignedBlocks.get(i), hashSha256);
        }

        files.put(path, new Inode(assignedBlocks, content.length));
    }

    private ImmutableIntArray findAvailableSectors(int requiredSectors) {
        List<Integer> assignedSectors = new ArrayList<>(requiredSectors);

        int sectorNumber = 0;

        for (int i = 0; i < requiredSectors; i++) {
            int clearSector = dataBlockAvailability.nextClearBit(sectorNumber);
            assignedSectors.add(clearSector);
            dataBlockAvailability.flip(clearSector);
            sectorNumber = clearSector;
        }

        return ImmutableIntArray.copyOf(assignedSectors);
    }

    public byte[] read(UUID path) throws IOException {
        Inode inode = files.get(path);

        byte[] content = new byte[inode.byteSize()];
        for (int i = 0; i < inode.dataBlocksNumbers().length(); i++) {
            int dataBlockNumber = inode.dataBlocksNumbers().get(i);
            byte[] block = storage.read(dataBlockNumber);
            byte[] currentBlockHash = calculateSha256(block);
            byte[] blockHash = dataBlocksHashes.get(dataBlockNumber);
            Preconditions.checkState(Arrays.equals(currentBlockHash, blockHash));

            long offset = (long) i * Constants.BLOCK_SIZE;

            if (i != inode.dataBlocksNumbers().length() - 1) {
                System.arraycopy(block, 0, content, (int) offset, block.length);
            } else {
                System.arraycopy(block, 0, content, (int) offset, inode.lastBlockSize());
            }
        }
        return content;
    }

    public void delete(UUID path) {
        ImmutableIntArray takenSectors = files.get(path).dataBlocksNumbers();
        takenSectors.forEach(it -> {
            dataBlocksHashes.remove(it);
            this.dataBlockAvailability.clear(it);
        });

        files.remove(path);
    }

    public Integer getTakenSectorsSize() {
        return dataBlockAvailability.cardinality();
    }

    private byte[] calculateSha256(byte[] content) {
        digest.reset();
        digest.update(content);
        return digest.digest();
    }
}
