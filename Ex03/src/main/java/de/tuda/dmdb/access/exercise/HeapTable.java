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
	 * Constructs table from record prototype
	 *
	 * @param prototypeRecord
	 */
	public HeapTable(AbstractRecord prototypeRecord) {
		super(prototypeRecord);
	}

	@Override
	// Inserts a record in the last Page of the Heap-Table. If no
	// space is left in a page, a new page is created.
	public RecordIdentifier insert(AbstractRecord record) {
		// Check whether the last page has space for the new record
		if (!this.lastPage.recordFitsIntoPage(record)) {
			// if not, create a new Page with the
			RowPage newPage = new RowPage(record.getFixedLength());
			// Set the correct page number to the new page
			newPage.setPageNumber(this.pages.size());
			// add it to our "DBMS"
			this.addPage(newPage);
			// reference lastPage to the newly added page
			this.lastPage = newPage;
		}
		// get relevant data for our Row Identifier
		int slotNum = lastPage.insert(record);
		int pageNum = lastPage.getPageNumber();

		// Setup the RowIdentifier
		RecordIdentifier rid = new RecordIdentifier(pageNum, slotNum);
		return rid;
	}

	@Override
	// Returns a record by its pageNumber & slotNumber.
	public AbstractRecord lookup(int pageNumber, int slotNumber) {
		// Get wanted page
		AbstractPage toLookup = this.getPage(pageNumber);
		//Copy data of prototype to work with
		AbstractRecord prot = prototype.clone();
		//fill prot with data and return it
		toLookup.read(slotNumber, prot);
		return prot;
	}

}
