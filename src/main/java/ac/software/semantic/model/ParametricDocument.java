package ac.software.semantic.model;

import java.util.List;

import ac.software.semantic.model.base.SpecificationDocument;

public interface ParametricDocument extends SpecificationDocument {

	public List<DataServiceParameterValue> getParameters();
}
