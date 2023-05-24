package ac.software.semantic.service;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.PublishDocument;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.model.state.PublishState;

public interface PublishableContainer extends IntermediatePublishableContainer {

	public void publish() throws Exception;
	
	public void unpublish() throws Exception;
	
	default PublishState getPublishState() throws Exception {
		TripleStoreConfiguration vc = getDatasetTripleStoreVirtuosoConfiguration();

		if (vc != null) {
			return getPublishDocument().getPublishState(vc.getId());
		} else {
			throw new Exception("Dataset is not published.");
		}
	}
	
	default PublishState checkPublishState() throws Exception {
		TripleStoreConfiguration vc = getDatasetTripleStoreVirtuosoConfiguration();

		if (vc != null) {
			return getPublishDocument().checkPublishState(vc.getId());
		} else {
			throw new Exception("Dataset is not published.");
		}
	}
	
	default void removePublishState(PublishState ps) {
		getPublishDocument().removePublishState(ps);
	}
	
	default boolean isPublished() {
		ProcessStateContainer psc = getPublishDocument().getCurrentPublishState(getVirtuosoConfigurations().values());
		if (psc != null) {
			PublishState ps = (PublishState)psc.getProcessState();
			if (ps.getPublishState() == DatasetState.PUBLISHED_PUBLIC || ps.getPublishState() == DatasetState.PUBLISHED_PRIVATE || ps.getPublishState() == DatasetState.PUBLISHED) {
				return true;
			}
		} 
			
		return false;
	}
	
	public TaskType getPublishTask();
	
	public TaskType getUnpublishTask();
	
	public TaskType getRepublishTask();
	

}
