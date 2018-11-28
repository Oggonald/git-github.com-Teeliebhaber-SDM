package de.tuda.dmdb.sql.operator.exercise;

import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.sql.operator.TableScanBase;
import de.tuda.dmdb.storage.AbstractRecord;

import java.util.Iterator;

@SuppressWarnings("unused")
public class TableScan extends TableScanBase {

    public TableScan(AbstractTable table) {
        super(table);
    }

    @Override
    public void open() {
        this.tableIter = this.table.iterator();
        //TODO: implement this method
    }

    @Override
    public AbstractRecord next() {
        //TODO: implement this method

        while (this.tableIter.hasNext()) {

            return this.tableIter.next();
        }

        return null;
    }

    @Override
    public void close() {
        //TODO: implement this method
        this.tableIter = null;
    }

}
