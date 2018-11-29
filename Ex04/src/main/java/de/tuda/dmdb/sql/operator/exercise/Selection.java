package de.tuda.dmdb.sql.operator.exercise;

import de.tuda.dmdb.sql.operator.Operator;
import de.tuda.dmdb.sql.operator.SelectionBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

import java.util.Vector;

@SuppressWarnings("unused")
public class Selection extends SelectionBase {

    public Selection(Operator child, int attribute, AbstractSQLValue constant) {
        super(child, attribute, constant);
    }

    @Override
    public void open() {
        // Initialize the Operator
        this.getChild().open();
    }

    @Override
    public AbstractRecord next() {
        AbstractRecord record;
        //Check for the next Operator not equal null, in order iterate through
        while ((record = this.getChild().next()) != null) {
            if (record.getValue(attribute).equals(constant)) {
                // Return the Record if it matches
                return record;
            }
        }
        // Otherwise return null
        return null;
    }

    @Override
    public void close() {
        //Close everything again
        this.getChild().close();
    }
}
