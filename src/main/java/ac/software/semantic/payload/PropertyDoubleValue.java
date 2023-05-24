package ac.software.semantic.payload;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.AnnotationEditValue;
import ac.software.semantic.model.PathElement;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PropertyDoubleValue extends PropertyValue {

	private String target; // a iri
	
	public PropertyDoubleValue(List<PathElement> path, AnnotationEditValue value, String target) {
		super(path, value);
		
		this.target = target;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

}
