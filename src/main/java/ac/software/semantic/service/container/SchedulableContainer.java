package ac.software.semantic.service.container;

import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;

public interface SchedulableContainer<D extends SpecificationDocument, F extends Response> extends BaseContainer<D,F> {

	boolean isScheduled();
	
	public void schedule() throws Exception;
	
	public void unschedule() throws Exception;
}
