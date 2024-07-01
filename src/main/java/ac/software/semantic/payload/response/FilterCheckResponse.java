package ac.software.semantic.payload.response;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class FilterCheckResponse implements Response {
	
	private List<FilterFieldCheck> fields;

	public FilterCheckResponse() {
	}

	public List<FilterFieldCheck> getFields() {
		return fields;
	}

	public void setFields(List<FilterFieldCheck> fields) {
		this.fields = fields;
	}
	
	public void addField(FilterFieldCheck ffc) {
		if (this.fields == null) {
			this.fields = new ArrayList<>();
		}
		
		this.fields.add(ffc);
	}

}
