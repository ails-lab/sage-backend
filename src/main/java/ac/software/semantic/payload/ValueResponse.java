package ac.software.semantic.payload;

public class ValueResponse {
    private String value;
    private int count;
    private boolean isLiteral;
    
    public ValueResponse(String value, int count, boolean isLiteral) {
    	this.value = value;
    	this.count = count;
    	this.isLiteral = isLiteral;
    }
    
	public String getValue() {
		return value;
	}
	
	public void setValue(String value) {
		this.value = value;
	}
	
	
	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public boolean isLiteral() {
		return isLiteral;
	}

	public void setLiteral(boolean isLiteral) {
		this.isLiteral = isLiteral;
	}

}
