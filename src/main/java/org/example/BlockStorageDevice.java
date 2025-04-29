package org.example;

import java.io.FileNotFoundException;

public interface BlockStorageDevice {

    public byte[] read(int blockNumber) throws FileNotFoundException;

    public void write(int blockNumber, byte[] data);
}
