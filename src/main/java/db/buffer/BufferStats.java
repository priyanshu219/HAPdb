package db.buffer;

public class BufferStats {
    private static int bufferReads;
    private static int bufferWrites;
    private static int cacheHits;
    private static int cacheMisses;

    public BufferStats() {
        bufferWrites = 0;
        bufferReads = 0;
        cacheMisses = 0;
        cacheHits = 0;
    }

    public static void incrementBufferReads() {
        bufferReads++;
    }

    public static void incrementBufferWrites() {
        bufferWrites++;
    }

    public static void incrementCacheHit() {
        cacheHits++;
    }

    public static void incrementCacheMiss() {
        cacheMisses++;
    }

    public static String statistics() {
        return "bufferReads: " + bufferReads + "\n" +
                "bufferWrites: " + bufferWrites + "\n" +
                "cacheHits: " + cacheHits + "\n" +
                "cacheMisses: " + cacheMisses + "\n";
    }


}
