package de.tuda.dmdb.storage.types.exercise;

import de.tuda.dmdb.storage.types.SQLIntegerBase;

/**
 * SQL integer value
 * @author cbinnig
 *
 */
public class SQLInteger extends SQLIntegerBase {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor with default value
	 */
	public SQLInteger(){
		super();
	}

	/**
	 * Constructor with value
	 * @param value Integer value
	 */
	public SQLInteger(int value){
		super(value);
	}

	@Override
	public byte[] serialize() {
		// Shift bytes to serialize
		byte[] tmp = new byte[4];
		tmp[0] = (byte) (this.value>>24);
		tmp[1] = (byte) (this.value>>16);
		tmp[2] = (byte) (this.value>>8);
		tmp[3] = (byte) (this.value);
		return tmp;
	}

	@Override
	public void deserialize(byte[] data) {
		// Shift and Apply Bitmask on the serialized
		// values in order to get the correct Value back
		int first = data[0];
		first = first<<24;
		first = first & 0xff000000;
		int second = data[1];
		second = second<<16;
		second = second & 0x00ff0000;
		int third = data[2];
		third = third<<8;
		third = third & 0x0000ff00;
		int fourth = data[3];
		fourth = fourth & 0x000000ff;
		int x1 = first|second;
		int x2 = third|fourth;
		int retu = x1|x2;
		this.value = retu;
	}


	@Override
	public SQLInteger clone(){
		return new SQLInteger(this.value);
	}

}
