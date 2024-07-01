package ac.software.semantic.service;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.User;
import ac.software.semantic.model.constants.type.DocumentType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.repository.core.UserRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.container.MappingObjectIdentifier;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;

@Service
public class DocumentService {
	
	@Autowired
	private UserRepository userRepository;

	@Autowired
	private DatasetService datasetService;
	
	@Autowired
	private IndexService indexService;

	@Autowired
	private MappingService mappingService;

	@Autowired
	private AnnotatorService annotatorService;

	@Autowired
	private EmbedderService embedderService;

	@Autowired
	private PagedAnnotationValidationService pavService;

	@Autowired
	private FilterAnnotationValidationService favService;

	@Autowired
	private UserTaskService userTaskService;

	@Autowired
	private FileService fileService;

    public EnclosedObjectContainer<?,?,?> getContainer(TaskDescription t) {
    	DocumentType dt = TaskType.getDocumentType(t.getType());
    	
   	    Optional<User> userOpt = userRepository.findById(t.getUserId());
   	    UserPrincipal currentUser = null;
   	    if (userOpt.isPresent()) {
   	    	currentUser = new UserPrincipal(userOpt.get(), null);
   	    } else {
   	    	return null;
   	    }
   	    	
   	    EnclosedObjectContainer<?,?,?> oc = null;
    	 
    	if (dt == DocumentType.DATASET) {
    		oc = datasetService.getContainer(currentUser, new SimpleObjectIdentifier(t.getDatasetId()));
    	} else if (dt == DocumentType.MAPPING) {
    		oc = mappingService.getContainer(currentUser, new MappingObjectIdentifier(t.getMappingId(), t.getMappingInstanceId()));
    	} else if (dt == DocumentType.FILE) {
    		oc = fileService.getContainer(currentUser, new SimpleObjectIdentifier(t.getFileId()));
    	} else if (dt == DocumentType.ANNOTATOR) {
    		oc = annotatorService.getContainer(currentUser, new SimpleObjectIdentifier(t.getAnnotatorId()));
    	} else if (dt == DocumentType.EMBEDDER) {
    		oc = embedderService.getContainer(currentUser, new SimpleObjectIdentifier(t.getEmbedderId()));
    	} else if (dt == DocumentType.PAGED_ANNOTATION_VALIDATION) {
    		oc = pavService.getContainer(currentUser, new SimpleObjectIdentifier(t.getPagedAnnotationValidationId()));
    	} else if (dt == DocumentType.FILTER_ANNOTATION_VALIDATION) {
    		oc = favService.getContainer(currentUser, new SimpleObjectIdentifier(t.getFilterAnnotationValidationId()));
    	} else if (dt == DocumentType.INDEX) {
    		oc = indexService.getContainer(currentUser, new SimpleObjectIdentifier(t.getIndexId()));
    	} else if (dt == DocumentType.USER_TASK) {
    		oc = userTaskService.getContainer(currentUser, new SimpleObjectIdentifier(t.getUserTaskId()));
    	}

    	return oc;
    }
}
