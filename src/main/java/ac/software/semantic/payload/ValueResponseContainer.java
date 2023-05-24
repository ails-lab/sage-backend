package ac.software.semantic.payload;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ValueResponseContainer<T> {
    private List<T> values;
    
    private int totalCount;
    private int distinctTotalCount;
    
    private int distinctSourceTotalCount;
    private int distinctValueTotalCount;
    
    private int page;
    private int pageSize;
    
	public List<T> getValues() {
		return values;
	}
	
	public void setValues(List<T> values) {
		this.values = values;
	}
	
	public int getTotalCount() {
		return totalCount;
	}
	
	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}
	
	public int getPage() {
		return page;
	}
	
	public void setPage(int page) {
		this.page = page;
	}
	
	public int getPageSize() {
		return pageSize;
	}
	
	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getDistinctTotalCount() {
		return distinctTotalCount;
	}

	public void setDistinctTotalCount(int distinctTotalCount) {
		this.distinctTotalCount = distinctTotalCount;
	}

	public int getDistinctSourceTotalCount() {
		return distinctSourceTotalCount;
	}

	public void setDistinctSourceTotalCount(int distinctSourceTotalCount) {
		this.distinctSourceTotalCount = distinctSourceTotalCount;
	}

	public int getDistinctValueTotalCount() {
		return distinctValueTotalCount;
	}

	public void setDistinctValueTotalCount(int distinctValueTotalCount) {
		this.distinctValueTotalCount = distinctValueTotalCount;
	}

}
