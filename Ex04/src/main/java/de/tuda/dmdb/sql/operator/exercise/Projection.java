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
        this.getChild().open();
        //TODO: implement this method

    }

    @Override
    public AbstractRecord next() {
        //TODO: implement this method
        AbstractRecord record;
        while ((record = this.getChild().next()) != null) {
            record.keepValues(attributes);
            
            return record;
        }

        return null;
    }

    @Override
    public void close() {
        //TODO: implement this method
        this.getChild().close();
    }
}
