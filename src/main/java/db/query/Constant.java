package db.query;

public class Constant implements Comparable<Constant> {
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

    @Override
    public boolean equals(Object obj) {
        Constant constant = (Constant) obj;
        return (null != intValue) ? (intValue.equals(constant.intValue)) : (stringValue.equals(constant.stringValue));
    }

    @Override
    public int compareTo(Constant constant) {
        return (null != intValue) ? (intValue.compareTo(constant.intValue)) : (stringValue.compareTo(constant.stringValue));
    }

    @Override
    public int hashCode() {
        return (null != intValue) ? (intValue.hashCode()) : (stringValue.hashCode());
    }

    @Override
    public String toString() {
        return (null != intValue) ? (intValue.toString()) : (stringValue.toString());
    }
}
