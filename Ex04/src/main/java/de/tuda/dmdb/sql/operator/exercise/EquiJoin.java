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
        this.getLeftChild().open();
        this.leftRecord = this.getLeftChild().next();
        this.getRightChild().open();
    }

    @Override
    public AbstractRecord next() {
        while (this.leftRecord != null) {
            AbstractRecord rightRecord = this.getRightChild().next();
            if (rightRecord != null) {
                if (this.leftRecord.getValue(leftAtt).equals(rightRecord.getValue(rightAtt))) {
                    AbstractRecord joined = this.leftRecord.clone();
                    joined = joined.append(rightRecord);
                    return joined;
                }
                continue;
            }
            this.leftRecord = this.getLeftChild().next();
            this.getRightChild().close();
            this.getRightChild().open();
        }
        return null;
    }

    @Override
    public void close() {
        this.getLeftChild().close();
        this.getRightChild().close();
    }
}
