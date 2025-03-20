package db.record;

public class RID {
    private final int blockNum;
    private final int slot;

    public RID(int blockNum, int slot) {
        this.blockNum = blockNum;
        this.slot = slot;
    }

    public int getBlockNum() {
        return blockNum;
    }

    public int getSlot() {
        return slot;
    }

    @Override
    public boolean equals(Object obj) {
        RID rid = (RID) obj;
        return (blockNum == rid.getBlockNum() && slot == rid.getSlot());
    }

    @Override
    public String toString() {
        return "RID{" +
                "blockNum=" + blockNum +
                ", slot=" + slot +
                '}';
    }
}
