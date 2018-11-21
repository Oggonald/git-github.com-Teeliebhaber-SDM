package de.tuda.dmdb.storage.exercise;

import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;


public class RowPage extends AbstractPage {

    /**
     * Constructir for a row page with a given (fixed) slot size
     *
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
        //Check if record does fit into the page
        // Throw an exception if this is not the case
        if(!recordFitsIntoPage(record)){
            throw new RuntimeException("There is no space left");
        }
        else{
            // init counter
            int writtenBytes = 0;

            if(doInsert){
                // If this flag is true, shift existing records to make space for the new record

                //Determine how far existing data has to be shifted
                int toShift = getNumRecords() - slotNumber;

                //Moooove data get out da way!
                System.arraycopy(data, slotNumber*slotSize, data, slotNumber*slotSize + slotSize, toShift*slotSize);

                // update Offset
                offset += record.getFixedLength();
                numRecords++;
            }
            // Otherwise, just insert the data as it is, without manipulating the position of
            // already inserted records
                for(int i = 0; i<record.getValues().length; i++){
                    // Same procedure as in regular insert method
                    if(record.getValue(i) instanceof SQLInteger){
                        byte[] toInsert = record.getValue(i).serialize();
                        for(int j = 0; j<toInsert.length;j++){
                            this.data[slotSize*slotNumber+j+writtenBytes] = toInsert[j];
                        }
                        writtenBytes+=SQLInteger.LENGTH;
                    }
                    else {
                        SQLInteger lengthToInstmp = new SQLInteger(record.getValue(i).getVariableLength());
                        byte[] lengthToIns = lengthToInstmp.serialize();
                        SQLInteger blabla = new SQLInteger(offsetEnd);
                        byte[] blablaByte = blabla.serialize();
                        for(int j = 0; j<4;j++){
                            this.data[slotNumber*slotSize+j+writtenBytes] = lengthToIns[j];
                            this.data[slotNumber*slotSize+4+j+writtenBytes] = blablaByte[j];
                        }
                        byte[] dings = record.getValue(i).serialize();
                        for (int x = record.getValue(i).getVariableLength() - 1; x>=0; x--){
                            this.data[offsetEnd] = dings[x];
                            offsetEnd--;
                        }
                        writtenBytes+=SQLVarchar.LENGTH;
                    }

                }
        }
    }

    @Override
    // Inserts a record at the end of the current page and updates the slot-size if there is still space left,
    // otherwise throws an exception
    public int insert(AbstractRecord record) {
        //Check whether the new record fits into the existing page
        if (recordFitsIntoPage(record)) {
            for (int i = 0; i < record.getValues().length; i++) {
                // Check whether the current value is a SQLInteger or SQLVarChar due to
                // different event handling
                // Here we are handling SQL Integers
                if (record.getValue(i) instanceof SQLInteger) {
                    // Get Value ready to be inserted
                    byte[] toInsert = record.getValue(i).serialize();
                    for (int j = 0; j < toInsert.length; j++) {
                        this.data[offset] = toInsert[j];
                        //update offset
                        offset++;
                    }
                } else {
                    // VarChar Handling - First: Get Length if the record
                    SQLInteger varRecordLength = new SQLInteger(record.getValue(i).getVariableLength());
                    byte[] lengthToIns = varRecordLength.serialize();
                    //Transform Offset into SQLInteger to keep track of new Offset
                    SQLInteger offsetSQLValue = new SQLInteger(offsetEnd);

                    byte[] offsetSQLByte = offsetSQLValue.serialize();
                    //Insert Records
                    for (int j = 0; j < 4; j++) {
                        this.data[offset] = lengthToIns[j];
                        this.data[offset + 4] = offsetSQLByte[j];
                        offset++;
                    }
                    //Update offset
                    offset += 4;
                    // Write varChar into page
                    byte[] varChar = record.getValue(i).serialize();
                    for (int x = record.getValue(i).getVariableLength() - 1; x >= 0; x--) {
                        this.data[offsetEnd] = varChar[x];
                        offsetEnd--;
                    }
                }

            }
            // Update number of records
            numRecords++;
        }
        return numRecords -1;
    }

    @Override
    // Fills the passed record-reference with values from the Page. (The record-reference specifies the SQL-datatypes).
    // An Exception is thrown if the specified slot is empty.
    public void read(int slotNumber, AbstractRecord record) {
        // One way to determine whether the page is full or not
        // if its full, throw an exception

        if ((this.getNumRecords() <= slotNumber)) {
            throw new RuntimeException("This Slot is empty!");
        } else {
            //Otherwise proceed as usual
            int writtenBytes = 0;

            for (int i = 0; i < record.getValues().length; i++) {
                //Determine if it is an Integer or a VarChar
                if (record.getValue(i) instanceof SQLInteger) {
                    // buffer buffers (what a surprise) our value
                    byte[] buffer = new byte[4];
                    System.arraycopy(data,
                            (slotSize * slotNumber) + writtenBytes,
                            buffer, 0,
                            SQLInteger.LENGTH);
                    // Create empty SQLInteger which gets filled later on (following two lines)
                    SQLInteger value = new SQLInteger();
                    //turn our byte array into a SQLInteger
                    value.deserialize(buffer);
                    record.setValue(i, value);
                    //Update our flag which keeps track of how many bytes are written by now
                    writtenBytes += SQLInteger.LENGTH;

                } else {
                    // Our Value is a VarChar
                    if (record.getValue(i) instanceof SQLVarchar) {
                        byte[] length = new byte[SQLInteger.LENGTH];
                        byte[] offSetVal = new byte[SQLInteger.LENGTH];
                        byte[] characters;
                        // Copy length of the VarChar in its designated buffer
                        System.arraycopy(data,
                                (slotNumber * slotSize) + writtenBytes,
                                length,
                                0,
                                SQLVarchar.LENGTH / 2);

                        // Copy offset of the VarChar in its designated buffer
                        System.arraycopy(data,
                                (slotNumber * slotSize) + writtenBytes + SQLVarchar.LENGTH / 2,
                                offSetVal,
                                0,
                                SQLVarchar.LENGTH / 2);

                        // Create Objects für buffers and fill them
                        SQLInteger lengthInt = new SQLInteger();
                        SQLInteger offsetInt = new SQLInteger();
                        lengthInt.deserialize(length);
                        offsetInt.deserialize(offSetVal);

                        // Fill buffer of actual content
                        characters = new byte[lengthInt.getValue()];
                        System.arraycopy(data,
                                offsetInt.getValue() - lengthInt.getValue() + 1,
                                characters,
                                0,
                                lengthInt.getValue());
                        // Create Object for actual Content
                        SQLVarchar value = new SQLVarchar(SQLInteger.LENGTH);
                        value.deserialize(characters);

                        //Write it into the record
                        record.setValue(i, value);

                        //Update counter
                        writtenBytes += SQLVarchar.LENGTH;
                    }
                }
            }
        }
    }


}
