package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     *
     * @param field1 The field index into the first tuple in the predicate
     * @param field2 The field index into the second tuple in the predicate
     * @param op     The operation to apply (as defined in Predicate.Op); either
     *               Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *               Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *               Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    private int field1;
    private Predicate.Op op;
    private int field2;

    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // saving variables
        this.field1 = field1;
        this.op = op;
        this.field2 = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     *
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        //check if either t1 and t2 are null, if so return null
        if (t1 == null || t2 == null) {
            return false;
        }
        // get the fields to compare from the tuples
        Field ff1 = t1.getField(field1);
        Field ff2 = t2.getField(field2);
        //compare the fields using the specified operator
        return ff1.compare(op, ff2);
    }

    public int getField1() {
        // return the saved variable field1;
        return this.field1;
    }

    public int getField2() {
        // return the saved variable field2;
        return this.field2;
    }

    public Predicate.Op getOperator() {
        // return the saved variable op;
        return this.op;
    }
}
