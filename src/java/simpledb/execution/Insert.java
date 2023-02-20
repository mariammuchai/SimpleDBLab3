package simpledb.execution;

import simpledb.transaction.TransactionId;
import simpledb.common.DbException;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.storage.Tuple;
import simpledb.common.Type;
import simpledb.storage.HeapFile;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.common.Database;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;





/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    //initializing variables
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private boolean called;
    private TupleDesc td;
    private boolean inserted = false;
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // done
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.called = false;
        TupleDesc childTd = child.getTupleDesc();
        HeapFile hf = (HeapFile) simpledb.common.Database.getCatalog().getDatabaseFile(tableId);
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Count"});
        if (!hf.getTupleDesc().equals(childTd)) {
            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
        }
    }

    public TupleDesc getTupleDesc() {
        // done
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // done
        super.open();
        child.open();
        called = false;
    }

    public void close() {
        // done
        super.close();
        child.close();
        called = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // done
        child.rewind();
        called = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // If we have already inserted the tuples, there's nothing more to fetch
        if (inserted) {
            return null;
        }

        int count = 0;
        List<Tuple> tuples = new ArrayList<>();

        while (child.hasNext()) {
            Tuple t = child.next();
            tuples.add(t);
        }

        try {
            HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
            for (Tuple t : tuples) {
                table.insertTuple(this.t, t);
                count++;
            }
        } catch (IOException e) {
            throw new DbException("Unable to insert tuple: " + e.getMessage());
        }

        Tuple result = new Tuple(getTupleDesc());
        result.setField(0, new IntField(count));
        inserted = true;
        return result;
    }


    @Override
    public OpIterator[] getChildren() {
        // done
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // done
        if (children.length > 0) {
            this.child = children[0];
        } else {
            this.child = null;
        }
    }
}
