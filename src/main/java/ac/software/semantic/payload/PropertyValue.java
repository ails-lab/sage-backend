package ac.software.semantic.payload;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.PathElement;
import edu.ntua.isci.ac.d2rml.model.RDFTerm;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PropertyValue {

	private List<PathElement> path;
	private RDFTerm value;
	
	public PropertyValue(List<PathElement> path, RDFTerm value) {
		this.path = path;
		this.value = value;
	}
	
	public List<PathElement> getPath() {
		return path;
	}
	
	public void setPath(List<PathElement> path) {
		this.path = path;
	}
	
	public RDFTerm getValue() {
		return value;
	}
	
	public void setValue(RDFTerm value) {
		this.value = value;
	}
}
