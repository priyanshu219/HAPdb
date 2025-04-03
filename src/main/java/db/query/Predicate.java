package db.query;

import db.planner.Plan;

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

    public int reductionFactor(Plan plan) {
        int factor = 1;
        for (Term term : terms) {
            factor *= term.reductionFactor(plan);
        }
        return factor;
    }

    public Constant equatesWithConstant(String fieldName) {
        for (Term term : terms) {
            Constant constant = term.equatesWithConstant(fieldName);
            if (null != constant) {
                return constant;
            }
        }
        return null;
    }

    public String equatesWithField(String fieldName) {
        for (Term term : terms) {
            String str = term.equatesWithField(fieldName);
            if (null != str) {
                return str;
            }
        }

        return null;
    }
}
