package ac.software.semantic.payload;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.PathElement;
import edu.ntua.isci.ac.d2rml.model.RDFTerm;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PropertyDoubleValue extends PropertyValue {

	private String target; // a iri
	
	public PropertyDoubleValue(List<PathElement> path, RDFTerm value, String target) {
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
