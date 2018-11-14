package de.tuda.dmdb.storage;

import org.junit.Assert;

import de.tuda.dmdb.storage.exercise.RowPage;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import de.tuda.dmdb.TestCase;


public class TestPage extends TestCase{
	public void testInsert1Record(){
		//insert record
		AbstractRecord r1 = new Record(2);
		r1.setValue(0, new SQLInteger(123456789));
		r1.setValue(1, new SQLVarchar("Test", 10));

		AbstractRecord r2 = new Record(2);
		r2.setValue(0, new SQLInteger(999999999));
		r2.setValue(1, new SQLVarchar("Hello", 10));

		AbstractRecord r3 = new Record(2);
		r3.setValue(0, new SQLInteger(222222222));
		r3.setValue(1, new SQLVarchar("hfasdlko", 10));

		AbstractRecord r4 = new Record(2);
		r4.setValue(0, new SQLInteger(333333333));
		r4.setValue(1, new SQLVarchar("over", 10));

		AbstractRecord r5 = new Record(2);
		r5.setValue(0, new SQLInteger(555555555));
		r5.setValue(1, new SQLVarchar("five", 10));

		AbstractRecord r6 = new Record(2);
		r6.setValue(0, new SQLInteger(666666666));
		r6.setValue(1, new SQLVarchar("devil", 10));

		AbstractPage p = new RowPage(r1.getFixedLength());
		try {
			p.insert(r1);
			p.insert(r2);
			p.insert(r3);
			p.insert(2,r4,true);
			p.insert(r5);
			p.insert(4, r6, false);
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}


		//read record
		AbstractRecord r1cmp = new Record(2);
		r1cmp.setValue(0, new SQLInteger());
		r1cmp.setValue(1, new SQLVarchar(10));

		AbstractRecord r2cmp = new Record(2);
		r2cmp.setValue(0, new SQLInteger());
		r2cmp.setValue(1, new SQLVarchar(10));

		AbstractRecord r3cmp = new Record(2);
		r3cmp.setValue(0, new SQLInteger());
		r3cmp.setValue(1, new SQLVarchar(10));

		AbstractRecord r4cmp = new Record(2);
		r4cmp.setValue(0, new SQLInteger());
		r4cmp.setValue(1, new SQLVarchar(10));

		AbstractRecord r5cmp = new Record(2);
		r5cmp.setValue(0, new SQLInteger());
		r5cmp.setValue(1, new SQLVarchar(10));

		try {
			p.read(0, r1cmp);
			p.read(1, r2cmp);
			p.read(3, r3cmp);
			p.read(2, r4cmp);
			p.read(4, r5cmp);
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}

		//compare
		Assert.assertEquals(r1, r1cmp);
		Assert.assertEquals(r2, r2cmp);
		Assert.assertEquals(r3, r3cmp);
		Assert.assertEquals(r4, r4cmp);
		Assert.assertEquals(r6, r5cmp);
	}

	public void testInsert2Record(){
		//insert record
		AbstractRecord r1 = new Record(4);
		r1.setValue(0, new SQLInteger(123456789));
		r1.setValue(1, new SQLVarchar("Test", 10));
		r1.setValue(2, new SQLInteger(123456789));
		r1.setValue(3, new SQLVarchar("Test", 10));

		AbstractRecord r2 = new Record(4);
		r2.setValue(0, new SQLInteger(999999999));
		r2.setValue(1, new SQLVarchar("Hello", 10));
		r2.setValue(2, new SQLInteger(999999999));
		r2.setValue(3, new SQLVarchar("Hello", 10));

		AbstractRecord r3 = new Record(4);
		r3.setValue(0, new SQLInteger(222222222));
		r3.setValue(1, new SQLVarchar("hfasdlko", 10));
		r3.setValue(2, new SQLInteger(222222222));
		r3.setValue(3, new SQLVarchar("hfasdlko", 10));

		AbstractRecord r4 = new Record(4);
		r4.setValue(0, new SQLInteger(333333333));
		r4.setValue(1, new SQLVarchar("over", 10));
		r4.setValue(2, new SQLInteger(333333333));
		r4.setValue(3, new SQLVarchar("over", 10));

		AbstractRecord r5 = new Record(4);
		r5.setValue(0, new SQLInteger(555555555));
		r5.setValue(1, new SQLVarchar("five", 10));
		r5.setValue(2, new SQLInteger(555555555));
		r5.setValue(3, new SQLVarchar("five", 10));

		AbstractRecord r6 = new Record(4);
		r6.setValue(0, new SQLInteger(666666666));
		r6.setValue(1, new SQLVarchar("devil", 10));
		r6.setValue(2, new SQLInteger(666666666));
		r6.setValue(3, new SQLVarchar("devil", 10));

		AbstractPage p = new RowPage(r1.getFixedLength());
		try {
			p.insert(r1);
			p.insert(r2);
			p.insert(r3);
			p.insert(2,r4,true);
			p.insert(r5);
			p.insert(4, r6, false);
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}


		//read record
		AbstractRecord r1cmp = new Record(4);
		r1cmp.setValue(0, new SQLInteger());
		r1cmp.setValue(1, new SQLVarchar(10));
		r1cmp.setValue(2, new SQLInteger());
		r1cmp.setValue(3, new SQLVarchar(10));

		AbstractRecord r2cmp = new Record(4);
		r2cmp.setValue(0, new SQLInteger());
		r2cmp.setValue(1, new SQLVarchar(10));
		r2cmp.setValue(2, new SQLInteger());
		r2cmp.setValue(3, new SQLVarchar(10));

		AbstractRecord r3cmp = new Record(4);
		r3cmp.setValue(0, new SQLInteger());
		r3cmp.setValue(1, new SQLVarchar(10));
		r3cmp.setValue(2, new SQLInteger());
		r3cmp.setValue(3, new SQLVarchar(10));

		AbstractRecord r4cmp = new Record(4);
		r4cmp.setValue(0, new SQLInteger());
		r4cmp.setValue(1, new SQLVarchar(10));
		r4cmp.setValue(2, new SQLInteger());
		r4cmp.setValue(3, new SQLVarchar(10));

		AbstractRecord r5cmp = new Record(4);
		r5cmp.setValue(0, new SQLInteger());
		r5cmp.setValue(1, new SQLVarchar(10));
		r5cmp.setValue(2, new SQLInteger());
		r5cmp.setValue(3, new SQLVarchar(10));

		try {
			p.read(0, r1cmp);
			p.read(1, r2cmp);
			p.read(3, r3cmp);
			p.read(2, r4cmp);
			p.read(4, r5cmp);
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}

		//compare
		Assert.assertEquals(r1, r1cmp);
		Assert.assertEquals(r2, r2cmp);
		Assert.assertEquals(r3, r3cmp);
		Assert.assertEquals(r4, r4cmp);
		Assert.assertEquals(r6, r5cmp);
	}
}
