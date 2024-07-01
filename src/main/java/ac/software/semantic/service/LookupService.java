package ac.software.semantic.service;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Pageable;

import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.service.lookup.LookupProperties;

public interface LookupService<D extends SpecificationDocument, F extends Response, L extends LookupProperties> extends ContainerService<D,F> {

	public ListPage<D> getAll(L lp, Pageable page);

	public ListPage<D> getAllByUser(ObjectId userId, L lp, Pageable page);
	
	public L createLookupProperties();
	
}
