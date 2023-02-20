package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.storage.IntField;
import simpledb.storage.Field;

import java.util.NoSuchElementException;
import java.util.List;
import java.util.ArrayList;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */

public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private OpIterator it;
    private TupleDesc td;
    private List<Tuple> aggregatedTuples;
    private boolean hasComputed = false;

    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // done
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.aggregatedTuples = new ArrayList<>();
        this.td = null;


        Type gfieldtype = gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield);

        if (child.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE) {
            aggregator = new StringAggregator(gfield, gfieldtype, afield, aop);
        } else {
            aggregator = new IntegerAggregator(gfield, gfieldtype, afield, aop);
        }

        it = aggregator.iterator();
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();

        if (gfieldtype != null) {
            types.add(gfieldtype);
            names.add(child.getTupleDesc().getFieldName(gfield));
        }

        types.add(child.getTupleDesc().getFieldType(afield));
        names.add(nameOfAggregatorOp(aop) + "(" + child.getTupleDesc().getFieldName(afield) + ")");

        if (aop.equals(Op.SUM_COUNT)) {
            types.add(Type.INT_TYPE);
            names.add("COUNT");
        }

        assert (types.size() == names.size());
        td = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));


    }


    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // done
        return this.gfield;
        }


    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        //done
        if (gfield == -1) {
            return null;
        }
        return td.getFieldName(0);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // done
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        // done
        if (gfield == -1) {
            return td.getFieldName(0);
        }
        return td.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // done
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        super.open();
        this.child.open();
        this.it = null;
        switch (this.aop) {
            case COUNT:
                this.aggregator.mergeTupleIntoGroup(getDefaultValueTuple(new IntField(0)));
                break;
            case SUM:
            case AVG:
                this.aggregator.mergeTupleIntoGroup(getDefaultValueTuple(new IntField(0)));
                break;
            case MIN:
                this.aggregator.mergeTupleIntoGroup(getDefaultValueTuple(null));
                break;
            case MAX:
                this.aggregator.mergeTupleIntoGroup(getDefaultValueTuple(null));
                break;
            default:
                throw new UnsupportedOperationException("unsupported operator");
        }
        while (this.child.hasNext())
            this.aggregator.mergeTupleIntoGroup(this.child.next());
        this.it = this.aggregator.iterator();
        this.it.open();
    }

    /**
     * Return a tuple with the default value for the aggregate field and
     * the group-by field, if any.
     *
     * @param defaultAggValue The default value for the aggregate field
     * @return A tuple with the default value for the aggregate field and
     * the group-by field, if any.
     */
    private Tuple getDefaultValueTuple(Field defaultAggValue) {
        Tuple tuple = new Tuple(this.td);
        if (this.gfield != Aggregator.NO_GROUPING) {
            tuple.setField(0, null);
            tuple.setField(1, defaultAggValue);
        } else {
            tuple.setField(0, defaultAggValue);
        }
        return tuple;
    }


    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.aggregatedTuples.isEmpty()) {
            // No more tuples to return
            return null;
        }

        Tuple resultTuple = new Tuple(this.td);
        if (this.gfield == Aggregator.NO_GROUPING) {
            // If there is no group by field, the result tuple should only have one field representing the aggregate result
            Field aggregateResult = this.aggregatedTuples.get(0).getField(1);
            resultTuple.setField(0, aggregateResult);
        } else {
            // If there is a group by field, the result tuple should have two fields: the group by field and the aggregate result
            Field groupByField = this.aggregatedTuples.get(0).getField(0);
            Field aggregateResult = this.aggregatedTuples.get(0).getField(1);
            resultTuple.setField(0, groupByField);
            resultTuple.setField(1, aggregateResult);
        }
        this.aggregatedTuples.remove(0); // Remove the tuple from the list since we have returned it
        return resultTuple;
    }


    public void rewind() throws DbException, TransactionAbortedException {
        // done
        // Rewind the child iterator
        child.rewind();
        // Reset the flag indicating if the aggregate has been computed
        hasComputed = false;
        // Reset the iterator that contains the computed aggregates
        it = null;
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // done
        return this.td;
    }

    public void close() {
        // done
        super.close();
        this.it = null;
    }

    @Override
    public OpIterator[] getChildren() {
        // done
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length != 1) {
            throw new IllegalArgumentException("Expected only one child");
        }

        this.child = children[0];
    }
}






