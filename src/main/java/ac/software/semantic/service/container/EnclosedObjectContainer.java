package ac.software.semantic.service.container;

import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;

public abstract class EnclosedObjectContainer<D extends SpecificationDocument, F extends Response, I extends EnclosingDocument> extends ObjectContainer<D,F> implements EnclosedBaseContainer<D,F, I> {
	
	protected I dataset;

	public I getEnclosingObject() {
//		if (dataset == null) {
			loadDataset(); // read fresh copy !!!
//		}
		
		return dataset;
	}

	public void setEnclosingObject(I dataset) {
		this.dataset = dataset;
	}
	
	protected abstract void loadDataset();
	

}
