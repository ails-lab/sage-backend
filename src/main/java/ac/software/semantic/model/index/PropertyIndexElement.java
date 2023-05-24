package ac.software.semantic.model.index;

import java.util.List;

import org.apache.jena.rdf.model.Property;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PropertyIndexElement {
	private String property; 
	private ClassIndexElement element;
//	private List<String> language;
	
//	private Integer index;
//	private String target;
	
	private List<IndexElementSelector> selectors;
	
	public PropertyIndexElement() {		
	}

	public PropertyIndexElement(String property, ClassIndexElement element) {
		this.property = property;
		this.element = element;
	}

	public PropertyIndexElement(Property property, ClassIndexElement element) {
		this.property = property.toString();
		this.element = element;
	}

	public PropertyIndexElement(String property) {
		this.property = property;
		this.element = null;
	}

	public PropertyIndexElement(Property property) {
		this.property = property.toString();
		this.element = null;
	}

	public String getProperty() {
		return property;
	}
	
	public void setProperty(String property) {
		this.property = property;
	}
	
	public ClassIndexElement getElement() {
		return element;
	}
	
	public void setElement(ClassIndexElement element) {
		this.element = element;
	}

//	public Integer getIndex() {
//		return index;
//	}
//
//	public void setIndex(Integer index) {
//		this.index = index;
//	}
//
//	public List<String> getLanguage() {
//		return language;
//	}

//	public void setLanguage(List<String> language) {
//		this.language = language;
//	}

//	public String getTarget() {
//		return target;
//	}
//
//	public void setTarget(String target) {
//		this.target = target;
//	}

	public List<IndexElementSelector> getSelectors() {
		return selectors;
	}

	public void setSelectors(List<IndexElementSelector> selectors) {
		this.selectors = selectors;
	}
}