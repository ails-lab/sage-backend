package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AnnotationEditGroupSearch {

	private List<AnnotationEditGroupSearchField> fields;
	
	private Integer minAnnotations;
	private Integer maxAnnotations;

	
	public AnnotationEditGroupSearch() {
		fields = new ArrayList<>();
	}

	public Integer getMinAnnotations() {
		return minAnnotations;
	}

	public void setMinAnnotations(Integer minAnnotations) {
		this.minAnnotations = minAnnotations;
	}

	public Integer getMaxAnnotations() {
		return maxAnnotations;
	}

	public void setMaxAnnotations(Integer maxAnnotations) {
		this.maxAnnotations = maxAnnotations;
	}

	public List<AnnotationEditGroupSearchField> getFields() {
		return fields;
	}

	public void setFields(List<AnnotationEditGroupSearchField> fields) {
		this.fields = fields;
	}
	
}
