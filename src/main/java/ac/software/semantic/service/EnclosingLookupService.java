package ac.software.semantic.service;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Pageable;

import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.service.lookup.LookupProperties;

public interface EnclosingLookupService<D extends SpecificationDocument, F extends Response, I extends EnclosingDocument, L extends LookupProperties> extends EnclosingService<D,F,I> {

	public ListPage<D> getAllByUser(List<I> enclosedIn, ObjectId userId, L lp, Pageable page);
	
	public ListPage<D> getAll(List<I> enclosedIn, L lp, Pageable page);
}
