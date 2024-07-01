package ac.software.semantic.model.expr;

import org.apache.jena.rdf.model.Resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import edu.ntua.isci.ac.d2rml.datatype.DatatypeUtils;
import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DimensionResult {

	@JsonIgnore
	private Dimension dimension;
	private Object value;
	
	@JsonIgnore
	private Resource datatype;
	
	public DimensionResult(Dimension dimension, Object value) {
		this.dimension = dimension;
		this.value = value;
		this.datatype = Vocabulary.resolve(dimension.getDatatype());
	}

	public Dimension getDimension() {
		return dimension;
	}

	public void setDimension(Dimension dimension) {
		this.dimension = dimension;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public Resource getDatatype() {
		return datatype;
	}

	public void setDatatype(Resource datatype) {
		this.datatype = datatype;
	}
	
//	public Object typedValue() {
//		return DatatypeUtils.typedValue(value, datatype);
//	}
	
	public String toString() {
		return "DimensionResult:"  + dimension.getName() + " / " + value + " / " + datatype; 
//		return dimension.getName() + " = " + value;
	}
}
