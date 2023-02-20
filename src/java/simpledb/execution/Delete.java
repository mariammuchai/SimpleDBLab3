package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    //initializing variables
    private TransactionId t;
    private OpIterator child;
    private boolean deleted;
    private boolean inserted = false;


    public Delete(TransactionId t, OpIterator child) {
        // done
        this.t = t;
        this.child = child;
        this.deleted = false;
    }

    public TupleDesc getTupleDesc() {
        // done
        //the tuple description for a delete operator is a single integer field
        Type[] typeArr = new Type[]{Type.INT_TYPE};
        return new TupleDesc(typeArr);
    }

    public void open() throws DbException, TransactionAbortedException {
        // Open the child operator
        child.open();
        super.open();
    }

    public void close() {
        // done
        //close the child operator
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // done
        //rewind the child operator
        child.rewind();
        deleted = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // If we have already deleted the tuples, there's nothing more to fetch
        if (inserted) {
            return null;
        }

        // Keep track of how many tuples we've deleted
        int count = 0;

        // Loop through all the tuples returned by the child operator
        while (child.hasNext()) {
            Tuple t = child.next();
            boolean deleted = false;

            // Delete the tuple from the database using the buffer pool
            try {
                Database.getBufferPool().deleteTuple(this.t, t);
                count++;
            } catch (IOException e) {
                // Throw an exception if we couldn't delete the Tuple
                throw new DbException("Unable to delete tuple");
            }

        }

        // Create a new tuple to hold the result
        Tuple result = new Tuple(getTupleDesc());
        // Set the first field of the result tuple to the number of tuples deleted
        result.setField(0, new IntField(count));
        // Mark that we've deleted the tuples so we don't delete them again
        inserted = true;
        // Return the result tuple
        return result;
    }






    @Override
    public OpIterator[] getChildren() {
        // done
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // done
        this.child = children[0];
    }

}
