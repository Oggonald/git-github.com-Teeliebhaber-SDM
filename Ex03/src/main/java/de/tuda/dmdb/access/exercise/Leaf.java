package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractIndexElement;
import de.tuda.dmdb.access.LeafBase;
import de.tuda.dmdb.access.RecordIdentifier;
import de.tuda.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Index leaf
 * @author cbinnig
 */
public class Leaf<T extends AbstractSQLValue> extends LeafBase<T>{

	/**
	 * Leaf constructor
	 * @param uniqueBPlusTree
	 */
	public Leaf(UniqueBPlusTreeBase<T> uniqueBPlusTree){
		super(uniqueBPlusTree);
	}

	/**
	 * @param key key of the record to find
	 * @return if found, returns the record, otherwise null
	 */
	@Override
	public AbstractRecord lookup(T key) {
		//find out if the key can even exist in the tree or is too big
        int pointer = this.binarySearch(key);
        if(pointer >= this.indexPage.getNumRecords()){
            return null;
        }
        AbstractRecord bla = this.uniqueBPlusTree.getLeafRecPrototype().clone();
        this.indexPage.read(pointer, bla);
        if(key.compareTo(bla.getValue(this.uniqueBPlusTree.KEY_POS)) == 0){
            int pageNumber = ((SQLInteger) bla.getValue(this.uniqueBPlusTree.PAGE_POS)).getValue();
            int slotNumber = ((SQLInteger) bla.getValue(this.uniqueBPlusTree.SLOT_POS)).getValue();
            return this.uniqueBPlusTree.getTable().lookup(pageNumber, slotNumber);
        }
		return null;
	}

	/**
	 * @param key key of the record
	 * @param record record to be inserted
	 * @return true if insertion was correct, false otherwise
	 */
	@Override
	public boolean insert(T key, AbstractRecord record){
		//TODO: implement this method
		//search for key and return false if existing
        if(lookup(key) != null){
            return false;
        }
        else{
            RecordIdentifier rid = this.getUniqueBPlusTree().getTable().insert(record);
            int pointer = this.binarySearch(key);
            AbstractRecord record2 = this.uniqueBPlusTree.getLeafRecPrototype().clone();
            record2.setValue(0, key);
            SQLInteger SQLpageNumber = new SQLInteger();
            SQLpageNumber.setValue(rid.getPageNumber());
            record2.setValue(1, SQLpageNumber);
            SQLInteger SQLslotNumber = new SQLInteger();
            SQLslotNumber.setValue(rid.getSlotNumber());
            record2.setValue(2, SQLslotNumber);
            this.indexPage.insert(pointer, record2, true);
        }
		return true;
	}
	
	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Leaf<T>(this.uniqueBPlusTree);
	}
}