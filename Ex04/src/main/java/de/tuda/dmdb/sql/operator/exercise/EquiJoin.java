package de.tuda.dmdb.sql.operator.exercise;

import de.tuda.dmdb.sql.operator.EquiJoinBase;
import de.tuda.dmdb.sql.operator.Operator;
import de.tuda.dmdb.storage.AbstractRecord;

public class EquiJoin extends EquiJoinBase {

    public EquiJoin(Operator leftChild, Operator rightChild, int leftAtt, int rightAtt) {
        super(leftChild, rightChild, leftAtt, rightAtt);
    }

    @Override
    public void open() {
        // Open left Child to initialize it
        this.getLeftChild().open();
        // Set left record to the next operator
        this.leftRecord = this.getLeftChild().next();
        // Open the right child to initialize it
        this.getRightChild().open();
    }

    @Override
    public AbstractRecord next() {
        // Break Condition: No More Elements of the left most side
        while (this.leftRecord != null) {
            // Get the next right record
            AbstractRecord rightRecord = this.getRightChild().next();
            // Check if the right Record is null
            // If not null, then compare the Attribute of the left Record with the Attribute of the right record
            if (rightRecord != null) {
                if (this.leftRecord.getValue(leftAtt).equals(rightRecord.getValue(rightAtt))) {
                    AbstractRecord joined = this.leftRecord.clone();
                    // If this the case, then create new Record, which will combine the left record with the right record
                    joined = joined.append(rightRecord);
                    return joined;
                }
                continue;
            }
            //otherwise get the next left child
            this.leftRecord = this.getLeftChild().next();
            this.getRightChild().close();
            this.getRightChild().open();
        }
        return null;
    }

    @Override
    public void close() {
        //free all used objects
        this.getLeftChild().close();
        this.getRightChild().close();
    }
}
