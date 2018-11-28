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
    @SuppressWarnings("unchecked Assignment")
    public void open() {
        this.tableIter = this.table.iterator();
    }

    @Override
    public AbstractRecord next() {
        if (this.tableIter.hasNext()) {
            return this.tableIter.next();
        }

        return null;
    }

    @Override
    public void close() {
        this.tableIter = null;
    }

}
