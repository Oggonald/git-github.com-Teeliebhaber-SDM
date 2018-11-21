package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractIndexElement;
import de.tuda.dmdb.access.NodeBase;
import de.tuda.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Index node
 * @author cbinnig
 *
 */
public class Node<T extends AbstractSQLValue> extends NodeBase<T>{

	/**
	 * Node constructor
	 * @param uniqueBPlusTree TODO
	 */
	public Node(UniqueBPlusTreeBase<T> uniqueBPlusTree){
		super(uniqueBPlusTree);
	}


	/**
	 * @param key key of the record to find
	 * @return if found, returns the record, otherwise null
	 */
	@Override
	public AbstractRecord lookup(T key) {
		//TODO: implement this method
        int pointer = this.binarySearch(key);
        if(pointer >= this.indexPage.getNumRecords()){
            return null;
        }
        AbstractRecord bla = this.uniqueBPlusTree.getNodeRecPrototype().clone();
        this.indexPage.read(pointer, bla);
        if(key.compareTo(bla.getValue(this.uniqueBPlusTree.KEY_POS)) <= 0){
            int pagePos = ((SQLInteger) bla.getValue(this.uniqueBPlusTree.PAGE_POS)).getValue();
            return this.uniqueBPlusTree.getIndexElement(pagePos).lookup(key);
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
        if(this.lookup(key) == null){
            return false;
        }
        else {
            int pointer = this.binarySearch(key, record);
            AbstractIndexElement pageToIns = this.uniqueBPlusTree.getIndexElement(pointer);
            if(pageToIns.isFull()){
                AbstractIndexElement aie1 = pageToIns.createInstance();
                AbstractIndexElement aie2 = pageToIns.createInstance();
                pageToIns.split(aie1, aie2);
                AbstractRecord aie1rec = this.uniqueBPlusTree.getNodeRecPrototype().clone();
                aie1rec.setValue(0, aie1.getMaxKey());
                SQLInteger pageNumber1 = new SQLInteger();
                pageNumber1.setValue(aie1.getPageNumber());
                aie1rec.setValue(1, pageNumber1);
                this.indexPage.insert(pointer, aie1rec, false);
                AbstractRecord aie2rec = this.uniqueBPlusTree.getNodeRecPrototype().clone();
                aie2rec.setValue(0, aie2.getMaxKey());
                SQLInteger pageNumber2 = new SQLInteger();
                pageNumber2.setValue(aie2.getPageNumber());
                aie2rec.setValue(1, pageNumber2);
                this.indexPage.insert(pointer + 1, aie2rec, true);
                this.insert(key, record);
            }
            else{
                pageToIns.insert(key, record);
                AbstractRecord oldMax = this.uniqueBPlusTree.getNodeRecPrototype().clone();
                this.getIndexPage().read(pointer, oldMax);
                if(pageToIns.getMaxKey().compareTo(oldMax.getValue(0)) > 0){
                    oldMax.setValue(0, pageToIns.getMaxKey());
                    this.indexPage.insert(pointer, oldMax, false);
                }
            }
        }
		return true;
	}
	
	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Node<T>(this.uniqueBPlusTree);
	}
	
}