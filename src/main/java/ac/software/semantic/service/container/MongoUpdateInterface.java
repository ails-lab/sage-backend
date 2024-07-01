package ac.software.semantic.service.container;

import ac.software.semantic.model.DatedDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;

@FunctionalInterface
public interface MongoUpdateInterface<D extends SpecificationDocument, F extends Response> {

	public void update(BaseContainer<D,F> oc) throws Exception;

}
