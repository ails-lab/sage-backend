package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

public class DependencyBinding {

	private String name;
	private List<ObjectId> value;
	
	public DependencyBinding() { 
		value = new ArrayList<>();
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<ObjectId> getValue() {
		return value;
	}

	public void setValue(List<ObjectId> value) {
		this.value = value;
	}
	
	public void addValue(ObjectId value) {
		this.value.add(value);
	}

}