package ac.software.semantic.payload;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.AnnotationEditValue;
import ac.software.semantic.model.PathElement;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PropertyValue {

	private List<PathElement> path;
	private AnnotationEditValue value;
	
	public PropertyValue(List<PathElement> path, AnnotationEditValue value) {
		this.path = path;
		this.value = value;
	}
	
	public List<PathElement> getPath() {
		return path;
	}
	
	public void setPath(List<PathElement> path) {
		this.path = path;
	}
	
	public AnnotationEditValue getValue() {
		return value;
	}
	
	public void setValue(AnnotationEditValue value) {
		this.value = value;
	}
}
