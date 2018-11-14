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
        //TODO: implement this method
        
    }

    @Override
    // Inserts a record at the end of the current page and updates the slot-size if there is still space left,
    // otherwise throws an exception
    public int insert(AbstractRecord record) {
        //TODO: implement this method
        if (recordFitsIntoPage(record)) {
            for (int i = 0; i < record.getValues().length; i++) {
                if (record.getValue(i) instanceof SQLInteger) {
                    byte[] toInsert = record.getValue(i).serialize();
                    for (int j = 0; j < toInsert.length; j++) {
                        this.data[offset] = toInsert[j];
                        offset++;
                    }
                } else {
                    SQLInteger lengthToInstmp = new SQLInteger(record.getValue(i).getVariableLength());
                    byte[] lengthToIns = lengthToInstmp.serialize();
                    SQLInteger blabla = new SQLInteger(offsetEnd);
                    byte[] blablaByte = blabla.serialize();
                    for (int j = 0; j < 4; j++) {
                        this.data[offset] = lengthToIns[j];
                        this.data[offset + 4] = blablaByte[j];
                        offset++;
                    }
                    offset += 4;
                    byte[] dings = record.getValue(i).serialize();
                    for (int x = record.getValue(i).getVariableLength() - 1; x >= 0; x--) {
                        this.data[offsetEnd] = dings[x];
                        offsetEnd--;
                    }
                }

            }
            numRecords++;
        }
        return 0;
    }

    @Override
    // Fills the passed record-reference with values from the Page. (The record-reference specifies the SQL-datatypes).
    // An Exception is thrown if the specified slot is empty.
    public void read(int slotNumber, AbstractRecord record) {
        //TODO: find further reasons to throw exceptions
        if ((this.getNumRecords() <= slotNumber)) {
            throw new RuntimeException("This Slot is empty!");
        } else {

            int writtenBytes = 0;
            for (int i = 0; i < record.getValues().length; i++) {
                if (record.getValue(i) instanceof SQLInteger) {
                    byte[] buffer = new byte[4];
                    System.arraycopy(data,
                            (slotSize * slotNumber) + writtenBytes,
                            buffer, 0,
                            SQLInteger.LENGTH);

                    SQLInteger value = new SQLInteger();
                    value.deserialize(buffer);
                    record.setValue(i, value);
                    writtenBytes += SQLInteger.LENGTH;

                } else {
                    if (record.getValue(i) instanceof SQLVarchar) {
                        byte[] length = new byte[SQLInteger.LENGTH];
                        byte[] offSetVal = new byte[SQLInteger.LENGTH];
                        byte[] characters;

                        System.arraycopy(data,
                                (slotNumber * slotSize) + writtenBytes,
                                length,
                                0,
                                SQLVarchar.LENGTH / 2);

                        System.arraycopy(data,
                                (slotNumber * slotSize) + writtenBytes + SQLVarchar.LENGTH / 2,
                                offSetVal,
                                0,
                                SQLVarchar.LENGTH / 2);

                        SQLInteger lengthInt = new SQLInteger();
                        SQLInteger offsetInt = new SQLInteger();

                        lengthInt.deserialize(length);
                        offsetInt.deserialize(offSetVal);
                        characters = new byte[lengthInt.getValue()];
                        System.arraycopy(data,
                                offsetInt.getValue() - lengthInt.getValue() + 1,
                                characters,
                                0,
                                lengthInt.getValue());

                        SQLVarchar value = new SQLVarchar(SQLInteger.LENGTH);
                        value.deserialize(characters);

                        record.setValue(i, value);
                        writtenBytes += SQLVarchar.LENGTH;
                    }
                }
            }
        }
    }


}
