package de.tuda.dmdb.sql.operator.exercise;

import java.util.Vector;

import de.tuda.dmdb.sql.operator.Operator;
import de.tuda.dmdb.sql.operator.ProjectionBase;
import de.tuda.dmdb.storage.AbstractRecord;

@SuppressWarnings("unused")
public class Projection extends ProjectionBase {

    public Projection(Operator child, Vector<Integer> attributes) {
        super(child, attributes);
    }

    @Override
    public void open() {
        // Initialize the operator
        this.getChild().open();

    }

    @Override
    public AbstractRecord next() {
        // Create a new record
        AbstractRecord record;
        // If the next record has a value, then proceed
        if ((record = this.getChild().next()) != null) {
            // Keep only the values which we need
            record.keepValues(attributes);
            // return the subset of attributes
            return record;
        }

        return null;
    }

    @Override
    public void close() {

        // free the objects
        this.getChild().close();
    }
}
