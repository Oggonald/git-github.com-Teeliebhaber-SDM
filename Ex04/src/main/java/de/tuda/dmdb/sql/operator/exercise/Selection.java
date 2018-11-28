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
        this.getChild().open();
    }

    @Override
    public AbstractRecord next() {
        AbstractRecord record;

        while ((record = this.getChild().next()) != null) {
            if (record.getValue(attribute).equals(constant)) {

                return record;
            }
        }
        return null;
    }

    @Override
    public void close() {
        this.getChild().close();
    }
}
