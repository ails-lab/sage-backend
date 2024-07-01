package ac.software.semantic.payload;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.AnnotatorContext;
import ac.software.semantic.model.DatasetContext;
//import ac.software.semantic.model.constants.ValidationType;
import ac.software.semantic.model.constants.type.AnnotationEditType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ValueAnnotationDetail {
	
	private String id;

	private String value;
	private String value2;
	private List<String> valueList;
	
	private List<Object> label;
	private List<Object> label2;
	
	private Map<String,Object> fields;
	
	private AnnotationEditType state;

	private Integer start;
	private Integer end;
	private Double score;
	
	private Integer othersRejected;
	private Integer othersAccepted;
	
	private Integer count;
	
//	private ValidationType validation;
	
	private List<ValueAnnotationReference> references;
	
	private Set<String> defaultTargets;
	
	private String selectedTarget;
	private String othersTarget;
	
	private Boolean manual;
	
	private AnnotatorContext annotatorInfo;
	
	private Object controlGraph;
	
	public ValueAnnotationDetail() {
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public AnnotationEditType getState() {
		return state;
	}

	public void setState(AnnotationEditType state) {
		this.state = state;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Integer getStart() {
		return start;
	}

	public void setStart(Integer start) {
		this.start = start;
	}

	public Integer getEnd() {
		return end;
	}

	public void setEnd(Integer end) {
		this.end = end;
	}

	public String getValue2() {
		return value2;
	}

	public void setValue2(String value2) {
		this.value2 = value2;
	}


	public Integer getOthersRejected() {
		return othersRejected;
	}


	public void setOthersRejected(Integer othersRejected) {
		this.othersRejected = othersRejected;
	}


	public Integer getOthersAccepted() {
		return othersAccepted;
	}


	public void setOthersAccepted(Integer othersAccepted) {
		this.othersAccepted = othersAccepted;
	}


	public Integer getCount() {
		return count;
	}


	public void setCount(Integer count) {
		this.count = count;
	}
	
	public void addCount(Integer count) {
		this.count += count;
	}


	public Double getScore() {
		return score;
	}


	public void setScore(Double score) {
		this.score = score;
	}


//	public ValidationType getValidation() {
//		return validation;
//	}
//
//
//	public void setValidation(ValidationType validation) {
//		this.validation = validation;
//	}

	public void addReference(ValueAnnotationReference ref) {
		if (references == null) {
			references = new ArrayList<>();
		}
		references.add(ref);
	}
	
	public List<ValueAnnotationReference> getReferences() {
		return references;
	}

	public void setReferences(List<ValueAnnotationReference> references) {
		this.references = references;
	}
	
	public void addDefaultTarget(String defaultTarget) {
		if (defaultTargets == null) {
			defaultTargets = new HashSet<>();
		}
		defaultTargets.add(defaultTarget);
	}
	
	public Set<String> getDefaultTargets() {
		return defaultTargets;
	}


	public String getSelectedTarget() {
		return selectedTarget;
	}


	public void setSelectedTarget(String selectedTarget) {
		this.selectedTarget = selectedTarget;
	}


	public String getOthersTarget() {
		return othersTarget;
	}


	public void setOthersTarget(String othersTarget) {
		this.othersTarget = othersTarget;
	}


	public Boolean isManual() {
		return manual;
	}


	public void setManual(Boolean manual) {
		this.manual = manual;
	}

	public AnnotatorContext getAnnotatorInfo() {
		return annotatorInfo;
	}

	public void setAnnotatorInfo(AnnotatorContext ai) {
		this.annotatorInfo = ai;
	}

	public List<Object> getLabel() {
		return label;
	}

	public void setLabel(List<Object> label) {
		this.label = label;
	}

	public List<Object> getLabel2() {
		return label2;
	}

	public void setLabel2(List<Object> label2) {
		this.label2 = label2;
	}

	public Map<String,Object> getFields() {
		return fields;
	}

	public void setFields(Map<String,Object> fields) {
		this.fields = fields;
	}
	
	public void addField(String property, Object value) {
		if (fields == null) {
			fields = new TreeMap<>();
		}
		fields.put(property, value);
		
	}

	public Object getControlGraph() {
		return controlGraph;
	}

	public void setControlGraph(Object controlGraph) {
		this.controlGraph = controlGraph;
	}

	public List<String> getValueList() {
		return valueList;
	}

	public void setValueList(List<String> valueList) {
		this.valueList = valueList;
	}

}
