package ac.software.semantic.payload;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.service.RDFTermHandler;
import edu.ntua.isci.ac.d2rml.model.RDFTerm;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ValueAnnotation  implements Response {
	
	private RDFTermHandler onValue;
	
    private List<ValueAnnotationDetail> details;
    
    private int count;
    
    private int index;
    
    private Object controlGraph;
    
    public ValueAnnotation() {
    	this.details = new ArrayList<>();
    }
    
	public List<ValueAnnotationDetail> getDetails() {
		return details;
	}

	public void setDetails(List<ValueAnnotationDetail> details) {
		this.details = details;
	}

	public RDFTermHandler getOnValue() {
		return onValue;
	}

	public void setOnValue(RDFTermHandler onValue) {
		this.onValue = onValue;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public Object getControlGraph() {
		return controlGraph;
	}

	public void setControlGraph(Object controlGraph) {
		this.controlGraph = controlGraph;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

}
