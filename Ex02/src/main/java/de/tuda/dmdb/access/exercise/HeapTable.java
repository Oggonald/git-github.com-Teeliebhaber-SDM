package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.access.RecordIdentifier;
import de.tuda.dmdb.access.HeapTableBase;

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
		return null;
	}

	@Override
	// Returns a record by its pageNumber & slotNumber.
	public AbstractRecord lookup(int pageNumber, int slotNumber) {
		//TODO: implement this method
		return null;
	}

}
