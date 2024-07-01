package ac.software.semantic.model.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.jena.rdf.model.Property;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataServiceParameter;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PropertyIndexElement {
	private String property; 
	
	private boolean inverse;

	private List<ClassIndexElement> elements;
	
	private List<IndexElementSelector> selectors;
	
	private boolean required;
	
	public PropertyIndexElement() {		
	}

	public PropertyIndexElement(String property, ClassIndexElement element) {
		this.property = property;
		this.elements = Arrays.asList(new ClassIndexElement [] { element });
	}

	public PropertyIndexElement(Property property, ClassIndexElement element) {
		this(property.toString(), element);
	}

	public PropertyIndexElement(String property) {
		this.property = property;
		this.elements = null;
	}

	public PropertyIndexElement(Property property) {
		this.property = property.toString();
		this.elements = null;
	}

	public String getProperty() {
		return property;
	}
	
	public void setProperty(String property) {
		this.property = property;
	}
	
	public List<ClassIndexElement> getElements() {
		return elements;
	}
	
	public void setElement(List<ClassIndexElement> elements) {
		this.elements = elements;
	}

	public List<IndexElementSelector> getSelectors() {
		return selectors;
	}

	public void setSelectors(List<IndexElementSelector> selectors) {
		this.selectors = selectors;
	}

	public boolean isInverse() {
		return inverse;
	}

	public void setInverse(boolean inverse) {
		this.inverse = inverse;
	}

	public boolean isRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}
	
	public boolean updateRequiredParameters(Map<Integer, DataServiceParameter> indexMap) {
		boolean res = false; 
		if (getElements() != null) {
			for (ClassIndexElement c : getElements()) {
				res = c.updateRequiredParameters(indexMap) | res;
			}
		}
		
		res = res || required;
			
		if (getSelectors() != null) {
			for (IndexElementSelector s : getSelectors()) {
				res = res || indexMap.get(s.getIndex()) == null || indexMap.get(s.getIndex()).isRequired();
			}
			
		}
		
		required = res;
		
		return res;
	}
	
	public void indexToPropertiesMap(Map<Integer, List<String>> map) {
		
		if (elements != null) {
			for (ClassIndexElement element : elements) {
				element.indexToPropertiesMap(map);
			}
		}
		
		if (selectors != null) {
			for (IndexElementSelector select : selectors) {
				List<String> list = map.get(select.getIndex());
				if (list == null) {
					list = new ArrayList<>();
					map.put(select.getIndex(), list);
				}
					
				list.add(getProperty());
			}
		}
	}

}