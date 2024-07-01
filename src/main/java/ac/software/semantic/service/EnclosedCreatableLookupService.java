package ac.software.semantic.service;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Pageable;

import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.request.UpdateRequest;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.service.lookup.LookupProperties;

public interface EnclosedCreatableLookupService<D extends SpecificationDocument, F extends Response, U extends UpdateRequest, I extends EnclosingDocument, L extends LookupProperties> 
	extends EnclosedCreatableService<D, F, U, I>,    LookupService<D, F, L> {
	
	public ListPage<D> getAllByUser(List<I> dataset, ObjectId userId, L lp, Pageable page);
}
