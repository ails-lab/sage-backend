package ac.software.semantic.payload.response;

import edu.ntua.isci.ac.d2rml.model.RDFTerm;

public class ValueResponse implements Response {
    private RDFTerm value;
    private int count;
//    private boolean isLiteral;
    
    public ValueResponse(RDFTerm value, int count) {
    	this.value = value;
    	this.count = count;
//    	this.isLiteral = isLiteral;
    }
    
	public RDFTerm getValue() {
		return value;
	}
	
	public void setValue(RDFTerm value) {
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
