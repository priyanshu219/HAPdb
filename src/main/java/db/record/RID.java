package db.record;

public record RID(int blockNum, int slot) {

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RID(int num, int slot1))) {
            return false;
        }
        return (blockNum == num && slot == slot1);
    }

    @Override
    public String toString() {
        return "RID{" +
                "blockNum=" + blockNum +
                ", slot=" + slot +
                '}';
    }
}
