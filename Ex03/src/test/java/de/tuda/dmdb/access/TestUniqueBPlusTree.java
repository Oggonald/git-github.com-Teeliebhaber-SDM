package de.tuda.dmdb.access;

import org.junit.Assert;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.access.exercise.UniqueBPlusTree;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import de.tuda.dmdb.TestCase;

import java.util.LinkedList;
import java.util.Random;

public class TestUniqueBPlusTree extends TestCase {

    /**
     * Insert four records and reads them again using a SQLInteger index
     */
    public void testIndexSimple() {
        AbstractRecord record1 = new Record(2);
        record1.setValue(0, new SQLInteger(1));
        record1.setValue(1, new SQLVarchar("Hello111", 10));

        AbstractRecord record2 = new Record(2);
        record2.setValue(0, new SQLInteger(2));
        record2.setValue(1, new SQLVarchar("Hello112", 10));

        AbstractRecord record3 = new Record(2);
        record3.setValue(0, new SQLInteger(3));
        record3.setValue(1, new SQLVarchar("Hello113", 10));

        AbstractRecord record4 = new Record(2);
        record4.setValue(0, new SQLInteger(4));
        record4.setValue(1, new SQLVarchar("Hello114", 10));

        AbstractTable table = new HeapTable(record1.clone());

        AbstractUniqueIndex<SQLInteger> index = new UniqueBPlusTree<SQLInteger>(table, 0, 1);
        index.insert(record1);
        index.insert(record2);
        index.insert(record3);
        index.insert(record4);
        //index.print();

        AbstractRecord record1Cmp = index.lookup((SQLInteger) record1.getValue(0));
        Assert.assertEquals(record1, record1Cmp);

        AbstractRecord record2Cmp = index.lookup((SQLInteger) record2.getValue(0));
        Assert.assertEquals(record2, record2Cmp);

        AbstractRecord record3Cmp = index.lookup((SQLInteger) record3.getValue(0));
        Assert.assertEquals(record3, record3Cmp);

        AbstractRecord record4Cmp = index.lookup((SQLInteger) record4.getValue(0));
        Assert.assertEquals(record4, record4Cmp);
    }

    /**
     * Insert three records and reads them again using a SQLVarchar index
     */
    public void testIndexSimple2() {
        AbstractRecord record1 = new Record(2);
        record1.setValue(0, new SQLInteger(1));
        record1.setValue(1, new SQLVarchar("Hello111", 10));

        AbstractRecord record2 = new Record(2);
        record2.setValue(0, new SQLInteger(2));
        record2.setValue(1, new SQLVarchar("Hello112", 10));

        AbstractRecord record3 = new Record(2);
        record3.setValue(0, new SQLInteger(3));
        record3.setValue(1, new SQLVarchar("Hello113", 10));

        AbstractTable table = new HeapTable(record1.clone());

        AbstractUniqueIndex<SQLVarchar> index = new UniqueBPlusTree<SQLVarchar>(table, 1, 1);
        index.insert(record1);
        index.insert(record2);
        index.insert(record3);
        index.print();

        AbstractRecord record1Cmp = index.lookup((SQLVarchar) record1.getValue(1));
        Assert.assertEquals(record1, record1Cmp);

        AbstractRecord record2Cmp = index.lookup((SQLVarchar) record2.getValue(1));
        Assert.assertEquals(record2, record2Cmp);

        AbstractRecord record3Cmp = index.lookup((SQLVarchar) record3.getValue(1));
        Assert.assertEquals(record3, record3Cmp);
    }

    /**
     * Insert three records and reads them again using a SQLVarchar index
     */
    public void testIndexSimple3() {
        AbstractRecord firstRecord = new Record(2);
        firstRecord.setValue(0, new SQLInteger(1));
        firstRecord.setValue(1, new SQLVarchar("Hello111", 10));

        AbstractRecord secondRecord = new Record(2);
        secondRecord.setValue(0, new SQLInteger(2));
        secondRecord.setValue(1, new SQLVarchar("Hello112", 10));


        AbstractTable table = new HeapTable(firstRecord.clone());

        AbstractUniqueIndex<SQLVarchar> index = new UniqueBPlusTree<SQLVarchar>(table, 1, 1);
        index.insert(firstRecord);
        index.insert(secondRecord);

        index.print();

        AbstractRecord firstRecordComparator = index.lookup((SQLVarchar) firstRecord.getValue(1));
        Assert.assertEquals(firstRecord, firstRecordComparator);

        AbstractRecord secondRecordComparator = index.lookup((SQLVarchar) secondRecord.getValue(1));
        Assert.assertEquals(secondRecord, secondRecordComparator);
    }

    public void testRootFilled() {
        AbstractRecord recordSample1 = new Record(2);
        recordSample1.setValue(0, new SQLInteger(1));
        recordSample1.setValue(1, new SQLVarchar("Auto111", 10));

        AbstractRecord recordSample2 = new Record(2);
        recordSample2.setValue(0, new SQLInteger(2));
        recordSample2.setValue(1, new SQLVarchar("Auto112", 10));

        AbstractRecord recordSample3 = new Record(2);
        recordSample3.setValue(0, new SQLInteger(3));
        recordSample3.setValue(1, new SQLVarchar("Auto113", 10));

        AbstractRecord recordSample4 = new Record(2);
        recordSample4.setValue(0, new SQLInteger(4));
        recordSample4.setValue(1, new SQLVarchar("Auto114", 10));

        AbstractRecord recordSample5 = new Record(2);
        recordSample5.setValue(0, new SQLInteger(5));
        recordSample5.setValue(1, new SQLVarchar("Auto115", 10));

        AbstractTable table = new HeapTable(recordSample1.clone());

        AbstractUniqueIndex<SQLInteger> index;
        index = new UniqueBPlusTree<>(table, 0, 1);

        index.insert(recordSample1);
        index.insert(recordSample2);
        index.insert(recordSample3);
        index.insert(recordSample4);
        index.insert(recordSample5);
        index.print();

        AbstractRecord firstComparator = index.lookup((SQLInteger) recordSample1.getValue(0));
        Assert.assertEquals(recordSample1, firstComparator);

        AbstractRecord secondComparator = index.lookup((SQLInteger) recordSample2.getValue(0));
        Assert.assertEquals(recordSample2, secondComparator);

        AbstractRecord thirdComparator = index.lookup((SQLInteger) recordSample3.getValue(0));
        Assert.assertEquals(recordSample3, thirdComparator);

        AbstractRecord fourthComparator = index.lookup((SQLInteger) recordSample4.getValue(0));
        Assert.assertEquals(recordSample4, fourthComparator);

        AbstractRecord fifthComparator = index.lookup((SQLInteger) recordSample5.getValue(0));
        Assert.assertEquals(recordSample5, fifthComparator);
    }


    public void testRootFilledTheSecond() {
        AbstractRecord record1 = new Record(2);
        record1.setValue(0, new SQLInteger(10));
        record1.setValue(1, new SQLVarchar("Hello111", 10));

        AbstractRecord record2 = new Record(2);
        record2.setValue(0, new SQLInteger(20));
        record2.setValue(1, new SQLVarchar("Hello112", 10));

        AbstractRecord record3 = new Record(2);
        record3.setValue(0, new SQLInteger(30));
        record3.setValue(1, new SQLVarchar("Hello113", 10));

        AbstractRecord record4 = new Record(2);
        record4.setValue(0, new SQLInteger(40));
        record4.setValue(1, new SQLVarchar("Hello114", 10));

        AbstractRecord record5 = new Record(2);
        record5.setValue(0, new SQLInteger(50));
        record5.setValue(1, new SQLVarchar("Hello115", 10));

        AbstractRecord record6 = new Record(2);
        record6.setValue(0, new SQLInteger(25));
        record6.setValue(1, new SQLVarchar("Hello1125", 10));

        AbstractRecord record7 = new Record(2);
        record7.setValue(0, new SQLInteger(28));
        record7.setValue(1, new SQLVarchar("Hello1128", 10));

        AbstractTable table = new HeapTable(record1.clone());

        AbstractUniqueIndex<SQLInteger> index = new UniqueBPlusTree<SQLInteger>(table, 0, 1);
        index.insert(record1);
        index.insert(record2);
        index.insert(record3);
        index.insert(record4);
        index.insert(record5);
        index.insert(record6);
        index.insert(record7);
        index.print();

        AbstractRecord record1Cmp = index.lookup((SQLInteger) record1.getValue(0));
        Assert.assertEquals(record1, record1Cmp);

        AbstractRecord record2Cmp = index.lookup((SQLInteger) record2.getValue(0));
        Assert.assertEquals(record2, record2Cmp);

        AbstractRecord record3Cmp = index.lookup((SQLInteger) record3.getValue(0));
        Assert.assertEquals(record3, record3Cmp);

        AbstractRecord record4Cmp = index.lookup((SQLInteger) record4.getValue(0));
        Assert.assertEquals(record4, record4Cmp);

        AbstractRecord record5Cmp = index.lookup((SQLInteger) record5.getValue(0));
        Assert.assertEquals(record5, record5Cmp);

        AbstractRecord record6Cmp = index.lookup((SQLInteger) record6.getValue(0));
        Assert.assertEquals(record6, record6Cmp);

        AbstractRecord record7Cmp = index.lookup((SQLInteger) record7.getValue(0));
        Assert.assertEquals(record7, record7Cmp);
    }

    private Record randomRecordGenerator() {
        Random rand = new Random();

        int randomNumberOne = rand.nextInt(99999) + 11111;
        int randomNumberTwo  = rand.nextInt(99999) + 11111;

        Record randomRecord = new Record(4);
        randomRecord.setValue(0, new SQLInteger(randomNumberOne));
        randomRecord.setValue(1, new SQLVarchar("Test", 10));
        randomRecord.setValue(2, new SQLInteger(randomNumberTwo));
        randomRecord.setValue(3, new SQLVarchar("TestTest", 10));

        return randomRecord;
    }

    public void testPenetrationOfRecords() {
        LinkedList<Record> testRecords = new LinkedList<>();
        LinkedList<Record> testRecordsCmp = new LinkedList<>();

        Record sample = randomRecordGenerator();

        AbstractTable table = new HeapTable(sample.clone());
        AbstractUniqueIndex<SQLInteger> index = new UniqueBPlusTree<>(table, 0, 2);

        try {
            for (int i = 0; i < 1000; i++) {
                Record record = randomRecordGenerator();
                Record recordCmp = new Record(4);

                recordCmp.setValue(0, new SQLInteger());
                recordCmp.setValue(1, new SQLVarchar(10));
                recordCmp.setValue(2, new SQLInteger());
                recordCmp.setValue(3, new SQLVarchar(10));

                testRecords.add(record);
                testRecordsCmp.add(recordCmp);

                index.insert(record);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());

            for (Record record : testRecords) {
                AbstractRecord recordCmp = index.lookup((SQLInteger) record.getValue(0));
                Assert.assertEquals(record, recordCmp);
            }
        }

        index.print();
    }

}
