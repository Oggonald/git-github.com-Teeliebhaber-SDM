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
        //find out if the key can even exist in the tree or is too big
        int pointer = this.binarySearch(key);
        if(pointer >= this.indexPage.getNumRecords()){
            return null;
        }
        //get a copy of the record to compare the entries to
        AbstractRecord bla = this.uniqueBPlusTree.getNodeRecPrototype().clone();
        this.indexPage.read(pointer, bla);
        //compare the keys, if the key is smaller it can still be in the leaf
        if(key.compareTo(bla.getValue(this.uniqueBPlusTree.KEY_POS)) <= 0){
            int pagePos = ((SQLInteger) bla.getValue(this.uniqueBPlusTree.PAGE_POS)).getValue();
            //look up the key in the child node/leaf
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
        //search for key and return false if existing
        if(this.lookup(key) != null){
            return false;
        }
        else {
            AbstractRecord copy = this.getUniqueBPlusTree().getNodeRecPrototype().clone();
            //find out which page it should be inserted to
            int pointer = this.binarySearch(key);
            this.indexPage.read(pointer, copy);
            AbstractIndexElement pageToIns = this.uniqueBPlusTree.getIndexElement(((SQLInteger) copy.getValue(1)).getValue());
            //if the page to insert it into is full, split the node/leaf before inserting
            if(pageToIns.isFull()){
                //create two new nodes/leafs
                AbstractIndexElement aie1 = pageToIns.createInstance();
                AbstractIndexElement aie2 = pageToIns.createInstance();
                //split the node/leaf into two
                pageToIns.split(aie1, aie2);
                //insert 2 new records into this node, each referencing one of the new child nodes/leafs
                AbstractRecord aie1rec = this.uniqueBPlusTree.getNodeRecPrototype().clone();
                aie1rec.setValue(0, aie1.getMaxKey());
                SQLInteger pageNumber1 = new SQLInteger();
                pageNumber1.setValue(aie1.getPageNumber());
                aie1rec.setValue(1, pageNumber1);
                //delete the old entry by writing over it
                this.indexPage.insert(pointer, aie1rec, false);
                AbstractRecord aie2rec = this.uniqueBPlusTree.getNodeRecPrototype().clone();
                aie2rec.setValue(0, aie2.getMaxKey());
                SQLInteger pageNumber2 = new SQLInteger();
                pageNumber2.setValue(aie2.getPageNumber());
                aie2rec.setValue(1, pageNumber2);
                //insert the second entry for the right child, shifting further existing records
                this.indexPage.insert(pointer+1, aie2rec, true);
                //call the insert function again
                this.insert(key, record);
            }
            else{
                //if the child is not yet full, call insert on it
                pageToIns.insert(key, record);
                AbstractRecord oldMax = this.uniqueBPlusTree.getNodeRecPrototype().clone();
                this.getIndexPage().read(pointer, oldMax);
                //if the new record has a higher max value, update the key in this node
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