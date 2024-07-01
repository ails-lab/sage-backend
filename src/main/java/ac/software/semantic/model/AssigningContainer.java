package ac.software.semantic.model;

import java.util.List;

import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.service.container.BaseContainer;

public interface AssigningContainer<D extends SpecificationDocument, F extends Response, A extends SpecificationDocument> extends BaseContainer<D,F> {

	public boolean isAssigned(A dataset, User user);
	
	public boolean assign(A assignment, User user);
	
	public boolean unassign(A assignment, User user);
	
	public int unassignAll(User user);
	
	public List<A> getAssigned(User user);
}
