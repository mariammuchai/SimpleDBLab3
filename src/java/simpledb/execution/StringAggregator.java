package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleIterator;
import simpledb.storage.Field;
import simpledb.storage.TupleDesc;
import simpledb.storage.IntField;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op op;
    private TupleDesc resultTD;
    private HashMap<Field, Integer> groupCounts;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // done
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Only COUNT operation is supported.");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.groupCounts = new HashMap<>();

        // Initialize the TupleDesc
        if (gbfield == Aggregator.NO_GROUPING) {
            resultTD = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            resultTD = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // Get the value of the group-by field from the tuple
        Field groupByField = gbfield == Aggregator.NO_GROUPING ? null : tup.getField(gbfield);

        // Increment the count for the corresponding group-by field
        if (groupCounts.containsKey(groupByField)) {
            groupCounts.put(groupByField, groupCounts.get(groupByField) + 1);
        } else {
            groupCounts.put(groupByField, 1);
        }
        // Assert that the count for the group-by field is greater than 0
        assert groupCounts.get(groupByField) > 0;
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // Create a new list to hold the tuples that will be returned by the iterator
        List<Tuple> tuples = new ArrayList<>();

        // Iterate over the groupCounts map to create a tuple for each group
        for (Map.Entry<Field, Integer> entry : groupCounts.entrySet()) {
            // Get the group-by field and the count for this group
            Field groupVal = entry.getKey();
            int groupCount = entry.getValue();

            // Create a new tuple with the result tuple descriptor
            Tuple t = new Tuple(resultTD);

            // If there is no grouping, set the aggregate value for the single tuple
            if (gbfield == Aggregator.NO_GROUPING) {
                t.setField(0, new IntField(groupCount));
            } else {
                // If there is grouping, set the group-by field value of this tuple
                t.setField(0, groupVal);
                // Set the aggregate value for this tuple as the count of the group
                t.setField(1, new IntField(groupCount));
            }

            // Add the tuple to the list of tuples
            tuples.add(t);
        }

        // Create a new TupleIterator with the result tuple descriptor and list of tuples
        return new TupleIterator(resultTD, tuples);
    }


}
