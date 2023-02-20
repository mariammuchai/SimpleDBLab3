package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    private Predicate p;
    private OpIterator child;
    public Filter(Predicate p, OpIterator child) {
        // saving variables
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // TODO: some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // return the TupleDesc of the child operator;
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // call open() method from of the operator class:
        super.open();
        //opening the child operator:
        child.open();
    }

    public void close() {
        // release resources held by the parent operator:
        super.close();
        //release any resources held by the child operator
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // resets the state of the filter operator and its child operator
       child.rewind();

    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        //If child operator is not open, throw an exception
        if (child == null)
            throw new NoSuchElementException();
        //keep looping until there are no more tuples to return
        while (child.hasNext()) {
            Tuple t = child.next();
            //evaluate the predicate on the tuple to check if it passes
            if (getPredicate().filter(t)) {
                return t;
            }
        }
        //no more tuples to return
        return null;

    }

    @Override
    public OpIterator[] getChildren() {
        //done
        OpIterator[] children = new OpIterator[1];
        children[0] = child;
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        //done
        if (children == null || children.length == 0) {
            return;
        }
        child =  children[0];
    }

}
