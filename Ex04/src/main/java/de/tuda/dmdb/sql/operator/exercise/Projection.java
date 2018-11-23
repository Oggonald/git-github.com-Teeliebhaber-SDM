package de.tuda.dmdb.sql.operator.exercise;

import java.util.Vector;

import de.tuda.dmdb.sql.operator.Operator;
import de.tuda.dmdb.sql.operator.ProjectionBase;
import de.tuda.dmdb.storage.AbstractRecord;

@SuppressWarnings("unused")
public class Projection extends ProjectionBase {
	
	public Projection(Operator child, Vector<Integer> attributes) {
		super(child, attributes);
	}

	@Override
	public void open() {
		//TODO: implement this method
	}

	@Override
	public AbstractRecord next() {
		//TODO: implement this method
		return null;
	}

	@Override
	public void close() {
		//TODO: implement this method
	}
}
