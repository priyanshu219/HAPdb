package db.query;

public class Constant {
    private final Integer intValue;
    private final String stringValue;

    public Constant(Integer intValue) {
        this.intValue = intValue;
        this.stringValue = null;
    }

    public Constant(String stringValue) {
        this.intValue = null;
        this.stringValue = stringValue;
    }

    public int asInt() {
        return intValue == null ? 0 : intValue;
    }

    public String asString() {
        return stringValue;
    }
}
