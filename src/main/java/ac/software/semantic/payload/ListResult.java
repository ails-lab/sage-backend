package ac.software.semantic.payload;

import java.util.List;

import ac.software.semantic.model.Pagination;
import ac.software.semantic.payload.response.Response;

public class ListResult<T extends Response> {
	private List<T> data;
	private Object metadata;
	private Pagination pagination;
	
	public ListResult(List<T> data, Pagination pagination) {
		this.data = data;
		this.pagination = pagination;
	}
	
	public List<T> getData() {
		return data;
	}
	
	public void setData(List<T> data) {
		this.data = data;
	}

	public Pagination getPagination() {
		return pagination;
	}

	public void setPagination(Pagination pagination) {
		this.pagination = pagination;
	}

	public Object getMetadata() {
		return metadata;
	}

	public void setMetadata(Object metadata) {
		this.metadata = metadata;
	}

}
