package de.tuda.dmdb.storage;

import org.junit.Assert;

import de.tuda.dmdb.storage.exercise.RowPage;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import de.tuda.dmdb.TestCase;


public class TestPage extends TestCase{
	public void testInsert1Record(){
		//insert
		AbstractRecord r1 = new Record(2);
		r1.setValue(0, new SQLInteger(123456789));
		r1.setValue(1, new SQLVarchar("Test", 10));

		AbstractRecord r2 = new Record(2);
		r2.setValue(0, new SQLInteger(999987328));
		r2.setValue(1, new SQLVarchar("NextTest", 10));

		AbstractRecord r3 = new Record(2);
		r3.setValue(0, new SQLInteger(222323232));
		r3.setValue(1, new SQLVarchar("TestoMato", 10));

		AbstractRecord r4 = new Record(2);
		r4.setValue(0, new SQLInteger(333333333));
		r4.setValue(1, new SQLVarchar("Aubergine", 10));

		AbstractRecord r5 = new Record(2);
		r5.setValue(0, new SQLInteger(5555545));
		r5.setValue(1, new SQLVarchar("fifthTest", 10));

		AbstractRecord r6 = new Record(2);
		r6.setValue(0, new SQLInteger(6666666));
		r6.setValue(1, new SQLVarchar("Omg", 10));

		AbstractPage p = new RowPage(r1.getFixedLength());

        p.insert(r1);
        p.insert(r2);
        p.insert(r3);
        p.insert(2,r4,true);
        p.insert(r5);
        p.insert(4, r6, false);

		//read
		AbstractRecord r1copy = new Record(2);
		r1copy.setValue(0, new SQLInteger());
		r1copy.setValue(1, new SQLVarchar(10));

		AbstractRecord r2copy = new Record(2);
		r2copy.setValue(0, new SQLInteger());
		r2copy.setValue(1, new SQLVarchar(10));

		AbstractRecord r3copy = new Record(2);
		r3copy.setValue(0, new SQLInteger());
		r3copy.setValue(1, new SQLVarchar(10));

		AbstractRecord r4copy = new Record(2);
		r4copy.setValue(0, new SQLInteger());
		r4copy.setValue(1, new SQLVarchar(10));

		AbstractRecord r5copy = new Record(2);
		r5copy.setValue(0, new SQLInteger());
		r5copy.setValue(1, new SQLVarchar(10));

        p.read(0, r1copy);
        p.read(1, r2copy);
        p.read(3, r3copy);
        p.read(2, r4copy);
        p.read(4, r5copy);

		//compare
		Assert.assertEquals(r1, r1copy);
		Assert.assertEquals(r2, r2copy);
		Assert.assertEquals(r3, r3copy);
		Assert.assertEquals(r4, r4copy);
		Assert.assertEquals(r6, r5copy);
	}

	public void testInsert2Record(){
		//insert record
		AbstractRecord r1 = new Record(4);
		r1.setValue(0, new SQLInteger(123456789));
		r1.setValue(1, new SQLVarchar("Test", 10));
		r1.setValue(2, new SQLInteger(123456789));
		r1.setValue(3, new SQLVarchar("Test", 10));

		AbstractRecord r2 = new Record(4);
		r2.setValue(0, new SQLInteger(999987328));
		r2.setValue(1, new SQLVarchar("NextTest", 10));
		r2.setValue(2, new SQLInteger(999987328));
		r2.setValue(3, new SQLVarchar("NextTest", 10));

		AbstractRecord r3 = new Record(4);
		r3.setValue(0, new SQLInteger(222323232));
		r3.setValue(1, new SQLVarchar("Testomato", 10));
		r3.setValue(2, new SQLInteger(222323232));
		r3.setValue(3, new SQLVarchar("Testomato", 10));


		AbstractPage p = new RowPage(r1.getFixedLength());

        p.insert(r1);
        p.insert(r2);
        p.insert(1,r3,true);

		//read record
		AbstractRecord r1copy = new Record(4);
		r1copy.setValue(0, new SQLInteger());
		r1copy.setValue(1, new SQLVarchar(10));
		r1copy.setValue(2, new SQLInteger());
		r1copy.setValue(3, new SQLVarchar(10));

		AbstractRecord r2copy = new Record(4);
		r2copy.setValue(0, new SQLInteger());
        r2copy.setValue(1, new SQLVarchar(10));
        r2copy.setValue(2, new SQLInteger());
        r2copy.setValue(3, new SQLVarchar(10));

		AbstractRecord r3copy = new Record(4);
        r3copy.setValue(0, new SQLInteger());
        r3copy.setValue(1, new SQLVarchar(10));
        r3copy.setValue(2, new SQLInteger());
        r3copy.setValue(3, new SQLVarchar(10));

        p.read(0, r1copy);
        p.read(1, r3copy);
        p.read(2, r2copy);

		//compare
		Assert.assertEquals(r1, r1copy);
		Assert.assertEquals(r2, r2copy);
		Assert.assertEquals(r3, r3copy);
	}
}
