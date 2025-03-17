package db.tx;

import db.buffer.Buffer;
import db.buffer.BufferManager;
import db.file.Block;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class BufferList {
    // TODO: Use pair instead of list
    private final Map<Block, Buffer> buffers;
    private final List<Block> pins;
    private final BufferManager bufferManager;

    public BufferList(BufferManager bufferManager) {
        this.bufferManager = bufferManager;
        this.pins = new ArrayList<>();
        this.buffers = new HashMap<>();
    }

    Buffer getBuffer(Block block) {
        return buffers.get(block);
    }

    void pin(Block block) throws IOException {
        Buffer buffer = bufferManager.pin(block);
        buffers.put(block, buffer);
        pins.add(block);
    }

    void unpin(Block block) {
        Buffer buffer = buffers.get(block);
        bufferManager.unpin(buffer);
        pins.remove(block);
        if (!pins.contains(block)) {
            buffers.remove(block);
        }
    }

    void unpinAll() {
        for (Block block : pins) {
            Buffer buffer = buffers.get(block);
            bufferManager.unpin(buffer);
        }
        buffers.clear();
        pins.clear();
    }
}
