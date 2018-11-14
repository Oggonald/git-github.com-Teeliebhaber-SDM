package de.tuda.dmdb.storage.exercise;

import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;


public class RowPage extends AbstractPage {

	/**
	 * Constructir for a row page with a given (fixed) slot size
	 * @param slotSize
	 */
	public RowPage(int slotSize) {
		super(slotSize);
	}
	
	@Override
	// Inserts a record at the specified slot-number. flag doInsert=true should insert while shifting
	// existing records, otherwise an in-place update should occur.
	// Exception is thrown if no space left (same as in insert(AbstractRecord))
	public void insert(int slotNumber, AbstractRecord record, boolean doInsert) {
		//TODO: implement this method
	}
	
	@Override
	// Inserts a record at the end of the current page and updates the slot-size if there is still space left,
	// otherwise throws an exception
	public int insert(AbstractRecord record){
		//TODO: implement this method
		if(!recordFitsIntoPage(record)){
		    throw new RuntimeException("This page is full");
        }else {


            numRecords++;
		}
		return 0;
	}

	@Override
	// Fills the passed record-reference with values from the Page. (The record-reference specifies the SQL-datatypes).
	// An Exception is thrown if the specified slot is empty.
	public void read(int slotNumber, AbstractRecord record){
		//TODO: implement this method
	}
}
