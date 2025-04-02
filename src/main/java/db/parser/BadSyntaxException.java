package db.parser;

public class BadSyntaxException extends RuntimeException {
    public BadSyntaxException() {
    }

    public BadSyntaxException(String message) {
        super(message);
    }
}
