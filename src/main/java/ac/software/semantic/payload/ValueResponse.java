package ac.software.semantic.payload;

import ac.software.semantic.model.AnnotationEditValue;

public class ValueResponse {
    private AnnotationEditValue value;
    private int count;
//    private boolean isLiteral;
    
    public ValueResponse(AnnotationEditValue value, int count) {
    	this.value = value;
    	this.count = count;
//    	this.isLiteral = isLiteral;
    }
    
	public AnnotationEditValue getValue() {
		return value;
	}
	
	public void setValue(AnnotationEditValue value) {
		this.value = value;
	}
	
	
	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

//	public boolean isLiteral() {
//		return isLiteral;
//	}
//
//	public void setLiteral(boolean isLiteral) {
//		this.isLiteral = isLiteral;
//	}

}
