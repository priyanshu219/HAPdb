package db.query;

import java.util.ArrayList;
import java.util.List;

public class Predicate {
    private final List<Term> terms;

    public Predicate() {
        terms = new ArrayList<>();
    }

    public Predicate(Term term) {
        terms = new ArrayList<>();
        terms.add(term);
    }

    public void coJoinWith(Predicate predicate) {
        terms.addAll(predicate.terms);
    }

    public boolean isSatisfied(Scan scan) {
        for (Term term : terms) {
            if (!term.isSatisfied(scan)) {
                return false;
            }
        }
        return true;
    }
}
