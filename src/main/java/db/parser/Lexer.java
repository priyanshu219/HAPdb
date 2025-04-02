package db.parser;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Set;

public class Lexer {
    private Set<String> keywords;
    private final StreamTokenizer token;

    public Lexer(String str) {
        initKeywords();
        token = new StreamTokenizer(new StringReader(str));
        token.ordinaryChar('.');
        token.wordChars('_', '_');
        token.lowerCaseMode(true);
        nextToken();
    }

    public boolean matchDelim(char delimiter) {
        return (delimiter == (char) token.ttype);
    }

    public boolean matchIntConstant() {
        return (token.ttype == StreamTokenizer.TT_NUMBER);
    }

    public boolean matchStringConstant() {
        return ('\'' == (char) token.ttype);
    }

    public boolean matchKeyword(String word) {
        return ((token.ttype == StreamTokenizer.TT_WORD) && token.sval.equals(word));
    }

    public boolean matchId() {
        return ((token.ttype == StreamTokenizer.TT_WORD) && !keywords.contains(token.sval));
    }

    public void eatDelim(char delimiter) throws BadSyntaxException {
        if (!matchDelim(delimiter)) {
            throw new BadSyntaxException();
        }
        nextToken();
    }

    public int eatIntConstant() throws BadSyntaxException {
        if (!matchIntConstant()) {
            throw new BadSyntaxException();
        }
        int intValue = (int) token.nval;
        nextToken();
        return intValue;
    }

    public String eatStringConstant() throws BadSyntaxException {
        if (!matchStringConstant()) {
            throw new BadSyntaxException();
        }
        String stringValue = token.sval;
        nextToken();
        return stringValue;
    }

    public void eatKeyword(String word) throws BadSyntaxException {
        if (!matchKeyword(word)) {
            throw new BadSyntaxException();
        }
        nextToken();
    }

    public String eatId() {
        if (!matchId()) {
            throw new BadSyntaxException();
        }
        String stringValue = token.sval;
        nextToken();
        return stringValue;
    }

    private void nextToken() {
        try {
            token.nextToken();
        } catch (IOException ex) {
            throw new BadSyntaxException();
        }
    }

    private void initKeywords() {
        keywords = Set.of("select", "from", "where", "and", "insert", "into", "values", "delete", "update", "set", "create", "table", "varchar", "int", "view", "as", "index", "on");
    }
}
