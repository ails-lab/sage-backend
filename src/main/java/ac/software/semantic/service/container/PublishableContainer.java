package ac.software.semantic.service.container;

import java.util.Properties;

import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.payload.response.Response;

public interface PublishableContainer<D extends SpecificationDocument, F extends Response, E extends ExecuteState, P extends PublishState<E>, I extends EnclosingDocument> 
                    extends IntermediatePublishableContainer<D, F, E, P, I> {

	public void publish(Properties props) throws Exception;
	
	public void unpublish(Properties props) throws Exception;
	
	default P getPublishState() throws Exception {
		TripleStoreConfiguration vc = getDatasetTripleStoreVirtuosoConfiguration();

		if (vc != null) {
			return getPublishDocument().getPublishState(vc.getId());
		} else {
			throw new Exception("Dataset is not published.");
		}
	}
	
	default P checkPublishState() {
		TripleStoreConfiguration vc = getDatasetTripleStoreVirtuosoConfiguration();

		if (vc != null) {
			return getPublishDocument().checkPublishState(vc.getId());
//		} else {
//			throw new Exception("Dataset is not published.");
		} else {
			return null;
		}
	}
	
	default void removePublishState(P ps) {
		getPublishDocument().removePublishState(ps);
	}
	
	default void removePublishState(P ps, Properties props) {
		removePublishState(ps);
	}
	
	public TaskType getPublishTask();
	
	public TaskType getUnpublishTask();
	
	public TaskType getRepublishTask();

}
