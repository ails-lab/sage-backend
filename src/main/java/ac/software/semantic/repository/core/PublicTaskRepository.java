package ac.software.semantic.repository.core;

import ac.software.semantic.model.PublicTask;
import ac.software.semantic.model.SavedTemplate;
import ac.software.semantic.model.constants.state.TaskState;
import ac.software.semantic.model.constants.type.PublicTaskType;
import ac.software.semantic.model.constants.type.TemplateType;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;
import java.util.Optional;

@Repository
public interface PublicTaskRepository extends MongoRepository<PublicTask, String> {
    
	Optional<PublicTask> findById(ObjectId id);

	Optional<PublicTask> findByUuid(String uuid);
	
	List<PublicTask> findByDatasetIdAndTypeAndFileSystemConfigurationId(ObjectId id, PublicTaskType type, ObjectId fileSystemConfigurationId);
	
	List<PublicTask> findByCompletedAtBeforeAndFileSystemConfigurationId(Date date, ObjectId fileSystemConfigurationId);
	
	List<PublicTask> findByStateAndFileSystemConfigurationId(TaskState state, ObjectId fileSystemConfigurationId);
	
}
