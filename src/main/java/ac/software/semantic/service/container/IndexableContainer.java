package ac.software.semantic.service.container;

import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.model.IndexDocument;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.NotificationMessage;
import ac.software.semantic.model.base.IndexableDocument;
import ac.software.semantic.model.constants.type.MessageType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.state.IndexState;

//public interface IndexableContainer extends BaseContainer {
//	
//	public IndexableDocument getIndexDocument();
//	
//	public IndexStructure getIndexStructure(IndexDocument idoc);
//	
////	default public IndexState getIndexStateByStructure(IndexStructure indexStructure) {
////		return getIndexDocument().getIndexStateByStructure(indexStructure.getElasticConfigurationId(), indexStructure.getId());
////	}
////
////	default public IndexState checkIndexStateByStructure(IndexStructure indexStructure) {
////		return getIndexDocument().checkIndexStateByStructure(indexStructure.getElasticConfigurationId(), indexStructure.getId());
////	}
//
//	default public IndexState getIndexStateByIndex(IndexDocument idoc) {
//		return getIndexDocument().getIndexStateByIndex(idoc.getId(), idoc.getElasticConfigurationId());
//	}
//
//	default public IndexState checkIndexStateByIndex(IndexDocument idoc) {
//		return getIndexDocument().checkIndexStateByIndex(idoc.getId());
//	}
//	
//	default public List<IndexState> checkIndexStates(ObjectId elasticConfigurationId) {
//		return getIndexDocument().checkIndexStates(elasticConfigurationId);
//	}
//
//	default public void removeIndexState(IndexState is) {
//		getIndexDocument().removeIndexState(is);
//	}
//	
//	public TaskType getIndexTask();
//	
//	public TaskType getUnindexTask();
//
//	default public void failIndexingByIndex(IndexDocument idoc) throws Exception {			
//		save(iic -> {			
//			IndexState ies = ((IndexContainer)iic).getIndexDocument().checkIndexStateByIndex(idoc.getId());
//	
//			if (ies != null) {
//				ies.setIndexState(IndexingState.INDEXING_FAILED);
//				ies.setIndexCompletedAt(new Date());
//				ies.setMessage(new NotificationMessage(MessageType.ERROR, "Unknown error."));
//			}
//		});
//	}
//}
