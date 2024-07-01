package ac.software.semantic.payload;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.payload.response.ResultCount;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ValueResponseContainer<T> {
    private List<T> values;
    
    private Integer totalCount;
    private Integer distinctTotalCount;
    
    private Integer distinctSourceTotalCount;
//    private Integer distinctValueTotalCount;
    private List<ResultCount> distinctValueTotalCount;
    
    private Object extra;
    
	public List<T> getValues() {
		return values;
	}
	
	public void setValues(List<T> values) {
		this.values = values;
	}
	
	public Integer getTotalCount() {
		return totalCount;
	}
	
	public void setTotalCount(Integer totalCount) {
		this.totalCount = totalCount;
	}

	public Integer getDistinctTotalCount() {
		return distinctTotalCount;
	}

	public void setDistinctTotalCount(Integer distinctTotalCount) {
		this.distinctTotalCount = distinctTotalCount;
	}

	public Integer getDistinctSourceTotalCount() {
		return distinctSourceTotalCount;
	}

	public void setDistinctSourceTotalCount(int distinctSourceTotalCount) {
		this.distinctSourceTotalCount = distinctSourceTotalCount;
	}

//	public Integer getDistinctValueTotalCount() {
//		return distinctValueTotalCount;
//	}
//
//	public void setDistinctValueTotalCount(int distinctValueTotalCount) {
//		this.distinctValueTotalCount = distinctValueTotalCount;
//	}
	
	public List<ResultCount> getDistinctValueTotalCount() {
		return distinctValueTotalCount;
	}
	
	public void setDistinctValueTotalCount(List<ResultCount> distinctValueTotalCount) {
		this.distinctValueTotalCount = distinctValueTotalCount;
	}

	public void addDistinctValueTotalCount(ResultCount rc) {
		if (distinctValueTotalCount == null) {
			distinctValueTotalCount = new ArrayList<>();
		}
		distinctValueTotalCount.add(rc);
	}

	public Object getExtra() {
		return extra;
	}

	public void setExtra(Object extra) {
		this.extra = extra;
	}

}
