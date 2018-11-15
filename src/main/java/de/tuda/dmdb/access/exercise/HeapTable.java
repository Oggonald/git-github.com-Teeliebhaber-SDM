package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.access.RecordIdentifier;
import de.tuda.dmdb.access.HeapTableBase;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.exercise.RowPage;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;

public class HeapTable extends HeapTableBase {

	/**
	 * 
	 * Constructs table from record prototype
	 * @param prototypeRecord
	 */
	public HeapTable(AbstractRecord prototypeRecord) {
		super(prototypeRecord);
	}

	@Override
	// Inserts a record in the last Page of the Heap-Table. If no
	// space is left in a page, a new page is created.
	public RecordIdentifier insert(AbstractRecord record) {
		//TODO: implement this method
        if(!this.lastPage.recordFitsIntoPage(record)){
            RowPage newPage = new RowPage(record.getFixedLength());
            this.addPage(newPage);
            this.lastPage = newPage;
        }
        int slotNum = lastPage.insert(record);
        int pageNum = lastPage.getPageNumber();
        RecordIdentifier rid = new RecordIdentifier(pageNum, slotNum);
        return rid;
	}

	@Override
	// Returns a record by its pageNumber & slotNumber.
	public AbstractRecord lookup(int pageNumber, int slotNumber) {
		//TODO: implement this method
        AbstractPage toLookup = this.getPage(pageNumber);
        AbstractRecord prot = prototype.clone();
        toLookup.read(slotNumber, prot);
        return prot;
	}

}
