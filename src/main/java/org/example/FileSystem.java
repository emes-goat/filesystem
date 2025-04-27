package org.example;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class FileSystem {

    private final Integer DISK_SIZE = 5 * 1024 * 1024;
    private final Integer SECTOR_SIZE = 4 * 1024;
    private final Integer SECTOR_COUNT = DISK_SIZE / SECTOR_SIZE;

    private Map<Integer, byte[]> sectors = new HashMap<>();
    private Map<Integer, byte[]> sectorHashes = new HashMap<>();
    private Set<Integer> takenSectors = new HashSet<>();

    private Map<UUID, Set<Integer>> files = new HashMap<>();

    private MessageDigest digest = MessageDigest.getInstance("SHA-256");

    public FileSystem() throws NoSuchAlgorithmException {
    }

    public void save(UUID path, byte[] content) {
        int requiredSectors = (int) Math.ceil((double) content.length / (double) SECTOR_SIZE);
        List<List<Byte>> chunkedContent = Lists.partition(Bytes.asList(content), SECTOR_SIZE);
        List<Integer> assignedSectors = findAvailableSectors(requiredSectors);

        if (chunkedContent.size() != assignedSectors.size()) {
            throw new RuntimeException("Chunked content size mismatch");
        }

        for (int i = 0; i < chunkedContent.size(); i++) {
            byte[] sectorBytes = Bytes.toArray(chunkedContent.get(i));
            byte[] hashSha256 = calculateSha256(sectorBytes);

            sectors.put(assignedSectors.get(i), sectorBytes);
            sectorHashes.put(assignedSectors.get(i), hashSha256);
        }

        files.put(path, new HashSet<>(assignedSectors));
    }

    private List<Integer> findAvailableSectors(int requiredSectors) {
        List<Integer> assignedSectors = new ArrayList<>(requiredSectors);

        int sectorNumber = 0;
        while (assignedSectors.size() < requiredSectors) {
            if (sectorNumber >= SECTOR_COUNT) {
                throw new IllegalStateException("Not enough disk space");
            }

            if (!takenSectors.contains(sectorNumber)) {
                assignedSectors.add(sectorNumber);
                takenSectors.add(sectorNumber);
            }

            sectorNumber++;
        }

        return assignedSectors;
    }

    private byte[] calculateSha256(byte[] content) {
        digest.reset();
        digest.update(content);
        return digest.digest();
    }

    public byte[] read(UUID path) {
        Set<Integer> takenSectors = files.get(path);
        byte[] content = null;

        for (int takenSector : takenSectors) {
            byte[] sectorContent = sectors.get(takenSector);
            byte[] hashedSectorContent = sectorHashes.get(takenSector);

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
        Set<Integer> takenSectors = files.get(path);
        for (int takenSector : takenSectors) {
            sectors.remove(takenSector);
            sectorHashes.remove(takenSector);
        }
        this.takenSectors.removeAll(takenSectors);
        files.remove(path);
    }

    public Integer getTakenSectorsSize(){
        return takenSectors.size();
    }
}
