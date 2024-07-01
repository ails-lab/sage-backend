package ac.software.semantic.service.container;

import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;

public interface EnclosedBaseContainer<D extends SpecificationDocument, F extends Response, I extends EnclosingDocument> extends BaseContainer<D, F>, EnclosedContainer<D,I> {
	
//	public I getDataset();
	
	public I getEnclosingObject();
	
}
