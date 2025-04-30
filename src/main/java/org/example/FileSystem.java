package org.example;

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
    private final Integer SECTOR_SIZE = 4 * 1024;
    private final Integer SECTOR_COUNT = DISK_SIZE / SECTOR_SIZE;

    private final Map<Integer, byte[]> dataBlocksHashes = new HashMap<>();
    private final BitSet dataBlockAvailability = new BitSet(SECTOR_COUNT);
    private final Map<UUID, Inode> files = new HashMap<>();

    private final MessageDigest digest = MessageDigest.getInstance("SHA-256");

    private final BlockStorageDevice storage = new FileBasedBlockStorageDevice(
            Paths.get("disk.bin"), 100);

    public FileSystem() throws NoSuchAlgorithmException, IOException {
    }

    public void save(UUID path, byte[] content) throws IOException {
        int requiredSectors = (int) Math.ceil((double) content.length / (double) SECTOR_SIZE);
        List<List<Byte>> chunkedContent = Lists.partition(Bytes.asList(content), SECTOR_SIZE);

        ImmutableIntArray assignedSectors = findAvailableSectors(requiredSectors);

        if (chunkedContent.size() != assignedSectors.length()) {
            throw new RuntimeException("Chunked content size mismatch");
        }

        for (int i = 0; i < chunkedContent.size(); i++) {
            byte[] sectorBytes = Bytes.toArray(chunkedContent.get(i));
            byte[] hashSha256 = calculateSha256(sectorBytes);

            storage.write(assignedSectors.get(i), sectorBytes); //TODO extend to whole sector
            //TODO calculate hash over whole sector
            dataBlocksHashes.put(assignedSectors.get(i), hashSha256);
        }

        files.put(path, new Inode(assignedSectors, (long) content.length));
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
        Inode fileSectors = files.get(path);
        byte[] content = null;

        for (int i = 0; i < fileSectors.dataBlocksNumbers().length(); i++) {
            byte[] sectorContent = storage.read(fileSectors.dataBlocksNumbers().get(i));
            byte[] hashedSectorContent = dataBlocksHashes.get(fileSectors.dataBlocksNumbers().get(i));
            //TODO in last dataBlock read bytes only up to length of metadata size

            if (!Arrays.equals(calculateSha256(sectorContent), hashedSectorContent)) {
                throw new IllegalStateException("Hashes are not matching");
            }

            if (content == null) {
                content = sectorContent;
            } else {
                content = Bytes.concat(content, sectorContent);
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
