package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.dmdb.access.AbstractIndexElement;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.PageManager;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Unique B+-Tree implementation 
 * @author cbinnig
 *
 * @param <T>
 */
public class UniqueBPlusTree<T extends AbstractSQLValue> extends UniqueBPlusTreeBase<T> {
	
	/**
	 * Constructor of B+-Tree with user-defined fil-grade
	 * @param table Table to be indexed
	 * @param keyColumnNumber Number of unique column which should be indexed
	 * @param fillGrade fill grade of index
	 */
	public UniqueBPlusTree(AbstractTable table, int keyColumnNumber, int fillGrade) {
		super(table, keyColumnNumber, fillGrade);
	} 
	
	/**
	 * Constructor for B+-tree with default fill grade
	 * @param table table to be indexed 
	 * @param keyColumnNumber Number of unique column which should be indexed
	 */
	public UniqueBPlusTree(AbstractTable table, int keyColumnNumber) {
		this(table, keyColumnNumber, DEFAULT_FILL_GRADE);
	}

	/**
	 * @param record record to be inserted
	 * @return true if insertion was correct, false otherwise
	 */
	@SuppressWarnings({ "unchecked" })
	@Override
	public boolean insert(AbstractRecord record) {
		//insert record
        T key = (T) record.getValue(this.keyColumnNumber);
        //no duplicate records
        if(this.lookup(key) != null){
            return false;
        }
        if(this.getRoot().isFull()){
            //if root is full, split root into two
            AbstractIndexElement aie1 = this.getRoot().createInstance();
            AbstractIndexElement aie2 = this.getRoot().createInstance();
            //create a new root
            AbstractIndexElement newRoot = new Node<T>(this.getRoot().getUniqueBPlusTree());
            this.getRoot().split(aie1, aie2);
            //set 2 records for the newly created child nodes/leafs from old root, insert them into new root
            newRoot.setIndexPage(PageManager.createDefaultPage(newRoot.getUniqueBPlusTree().getNodeRecPrototype().getFixedLength()));
            AbstractRecord aie1rec = newRoot.getUniqueBPlusTree().getNodeRecPrototype().clone();
            aie1rec.setValue(0, aie1.getMaxKey());
            SQLInteger pageNumber1 = new SQLInteger();
            pageNumber1.setValue(aie1.getPageNumber());
            aie1rec.setValue(1, pageNumber1);
            newRoot.getIndexPage().insert(aie1rec);
            AbstractRecord aie2rec = newRoot.getUniqueBPlusTree().getNodeRecPrototype().clone();
            aie2rec.setValue(0, aie2.getMaxKey());
            SQLInteger pageNumber2 = new SQLInteger();
            pageNumber2.setValue(aie2.getPageNumber());
            aie2rec.setValue(1, pageNumber2);
            newRoot.getIndexPage().insert(aie2rec);
            //set root as newRoot
            this.setRoot(newRoot);
            //call insert on the new root
            this.getRoot().insert(key, record);
        }
        else {
            this.getRoot().insert(key, record);
        }
		return true;
	}

	/**
	 * @param key key of the record to find
	 * @return if found, returns the record, otherwise null
	 */
	@Override
	public AbstractRecord lookup(T key) {
		//get root and call lookup on it
		return this.getRoot().lookup(key);
	}

}
