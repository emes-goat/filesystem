package org.example;

import com.google.common.primitives.ImmutableIntArray;

public record Inode(
        ImmutableIntArray dataBlocksNumbers,
        Long byteSize
) {
}
