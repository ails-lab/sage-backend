package ac.software.semantic.payload.response;

public class ResultCount {
	private String field;
	
	private int count;
	
	public ResultCount(String field, int count) {
		this.field = field;
		this.count = count;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}
