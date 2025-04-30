package org.example;

import java.io.IOException;

public interface BlockStorageDevice {

    byte[] read(int blockNumber) throws IOException;

    void write(int blockNumber, byte[] data) throws IOException;
}
