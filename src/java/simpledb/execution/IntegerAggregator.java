package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.TupleDesc;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import simpledb.storage.TupleIterator;
import java.util.HashMap;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.Collections;




/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     * NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     * if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */
    //initializing variables:
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op op;

    private TupleDesc resultTD;
    private HashMap<Field, ArrayList<Integer>> groupFields;


    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // done; saving variables
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.groupFields = new HashMap<>();

        // Initialize the TupleDesc
        if (gbfield == NO_GROUPING) {
            resultTD = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            resultTD = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        //get the value of the group by field from the tuple
        Field groupByField = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        //get the list of group values for the group by field, creating a new one if it does not already exist
        ArrayList<Integer> groupVals = groupFields.computeIfAbsent(groupByField, k -> new ArrayList<>());
        //get the value of the aggregate field from the tuple
        int newFieldVal = ((IntField) tup.getField(afield)).getValue();
        //add one to the group if the operator is count
        if (op == Op.COUNT) {
            groupVals.add(1);
        } else {
            //If the group is empty, add the new aggregate value
            if (groupVals.isEmpty()) {
                groupVals.add(newFieldVal);
            } else {
                //otherwise, update the aggregatevalue according to the operator
                int aggregateVal = groupVals.get(0);
                switch (op) {
                    case SUM:
                        aggregateVal += newFieldVal;
                        break;
                    case AVG:
                        aggregateVal = (aggregateVal * groupVals.size() + newFieldVal) / (groupVals.size() + 1);
                        break;
                    case MIN:
                        aggregateVal = Math.min(aggregateVal, newFieldVal);
                        break;
                    case MAX:
                        aggregateVal = Math.max(aggregateVal, newFieldVal);
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid operator");
                }
                //set the updated aggregate value in the group list
                groupVals.set(0, aggregateVal);
            }
        }
    }



    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        List<Tuple> tuples = new ArrayList<>();
        //Iterate over all groups and create a tuple for each group
        for (Map.Entry<Field, ArrayList<Integer>> entry : groupFields.entrySet()) {
            Field groupVal = entry.getKey();
            ArrayList<Integer> groupVals = entry.getValue();
            //create a new tuple for this group
            Tuple t = new Tuple(resultTD);

            if (gbfield == NO_GROUPING) {
                //if there is no grouping, set the aggregate value for the single tuple
                t.setField(0, new IntField(groupVals.get(0)));
            } else {
                //if there is grouping, set the group-by field value of this tuple
                t.setField(0, groupVal);
                //set the aggregate value for this tuple based on the operator
                switch (op) {
                    case COUNT:
                        //For COUNT, set the number of values in the group
                        t.setField(1, new IntField(groupVals.size()));
                        break;
                        //for AVG, calculate the average of the values in the group
                    //and round to the nearest integer.
                    case AVG:
                        double avg = 0.0;
                        if (!groupVals.isEmpty()) {
                            int sum = groupVals.stream().mapToInt(Integer::intValue).sum();
                            avg = sum * 1.0 / groupVals.size();
                        }
                        t.setField(1, new IntField((int) Math.round(avg)));
                        break;
                    case SUM:
                    case MIN:
                    case MAX:
                        //For SUM, MIN, and MAX, set the corresponding aggregate value
                        if (!groupVals.isEmpty()) {
                            int aggValue;
                            if (op == Op.SUM) {
                                aggValue = groupVals.stream().mapToInt(Integer::intValue).sum();
                            } else if (op == Op.MIN) {
                                aggValue = Collections.min(groupVals);
                            } else {
                                aggValue = Collections.max(groupVals);
                            }
                            t.setField(1, new IntField(aggValue));
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid operator");
                }
            }
            //Add the tuple to the list of tuples
            tuples.add(t);
        }

        return new TupleIterator(resultTD, tuples);
    }

}

