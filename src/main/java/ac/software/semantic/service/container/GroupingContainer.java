package ac.software.semantic.service.container;

import ac.software.semantic.model.base.SpecificationDocument;

public interface GroupingContainer<D extends SpecificationDocument> {
	
	public void updateMaxGroup() throws Exception;

}
