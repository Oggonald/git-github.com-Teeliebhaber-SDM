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
        // Get the iterator
        this.tableIter = this.table.iterator();
    }

    @Override
    public AbstractRecord next() {
        // Return next element via the iterator if available
        if (this.tableIter.hasNext()) {
            return this.tableIter.next();
        }
        // Otherwise return null
        return null;
    }

    @Override
    public void close() {
        // Free the iterator object
        this.tableIter = null;
    }

}
