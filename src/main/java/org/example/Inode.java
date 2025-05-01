package org.example;

import com.google.common.primitives.ImmutableIntArray;

public record Inode(
        ImmutableIntArray dataBlocksNumbers,
        int byteSize
) {

    public int lastBlockSize() {
        return Math.toIntExact(byteSize % Constants.BLOCK_SIZE); //% - reminder of division operator
    }
}
