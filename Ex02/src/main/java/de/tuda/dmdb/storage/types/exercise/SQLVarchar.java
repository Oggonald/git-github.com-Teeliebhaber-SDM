package de.tuda.dmdb.storage.types.exercise;

import de.tuda.dmdb.storage.types.SQLVarcharBase;

/**
 * SQL varchar value
 * @author cbinnig
 *
 */
public class SQLVarchar extends SQLVarcharBase {	

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor with default value and max. length 
	 * @param maxLength
	 */
	public SQLVarchar(int maxLength){
		super(maxLength);

	}
	
	/**
	 * Constructor with string value and max. length 
	 * @param value
	 * @param maxLength
	 */
	public SQLVarchar(String value, int maxLength){
		super(value, maxLength);
	}
	
	@Override
	public byte[] serialize() {
		return this.value.getBytes();
	}

	@Override
	public void deserialize(byte[] data) {
		//TODO: implement this method
		String decoded = new String(data);
		this.value = decoded;
	}
	
	@Override
	public SQLVarchar clone(){
		return new SQLVarchar(this.value, this.maxLength);
	}

}
