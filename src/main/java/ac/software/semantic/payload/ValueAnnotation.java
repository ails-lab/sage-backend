package ac.software.semantic.payload;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import ac.software.semantic.model.AnnotationEditValue;
import ac.software.semantic.model.state.MappingState;

public class ValueAnnotation {
	
	private AnnotationEditValue onValue;
	
    private List<ValueAnnotationDetail> details;
    
    private int count;
    
    public ValueAnnotation() {
    	this.details = new ArrayList<>();
    }
    
	public List<ValueAnnotationDetail> getDetails() {
		return details;
	}

	public void setDetails(List<ValueAnnotationDetail> details) {
		this.details = details;
	}

	public AnnotationEditValue getOnValue() {
		return onValue;
	}

	public void setOnValue(AnnotationEditValue onValue) {
		this.onValue = onValue;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

}
