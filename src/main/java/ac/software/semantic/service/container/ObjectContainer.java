package ac.software.semantic.service.container;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;

import ac.software.semantic.model.DatedDocument;
import ac.software.semantic.model.IdentifiableDocument;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ContainerService;
import ac.software.semantic.service.IdentifiableDocumentService;
import ac.software.semantic.service.TaskSpecification;
import ac.software.semantic.service.exception.StateConflictException;
import ac.software.semantic.service.exception.TaskConflictException;

public abstract class ObjectContainer<T extends SpecificationDocument, F extends Response> implements BaseContainer<T,F> {
	
	protected T object;
	
	protected UserPrincipal currentUser;
	protected UserPrincipal objectCreator;

	@Override
	public T getObject() {
		return object;
	}
	
	public void setObject(T object) {
		this.object = object;
	}

	public abstract String getDescription();

	@Override
	public UserPrincipal getCurrentUser() {
		return currentUser;
	}

	public String containerString() {
		return getClass().getName();
	}
	
	@Override
	public ObjectId getSecondaryId() {
		return null;
	}
	
	public String synchronizationString() {
		return synchronizationString(new TaskType[] {});
	}

	// TODO should check different synchronization strings ( use of file system / triples store ) depending on task type
	public String synchronizationString(List<TaskType> taskType) {
		return synchronizationString(taskType.toArray(new TaskType[] {}));
	}

	private String synchronizationString(TaskType... taskType) {
		String res = "SYNC:TASK:" + containerString() + ":";
		
		if (taskType.length > 0) {
			
			String[] sTaskType = new String[taskType.length];
			for (int i = 0; i < taskType.length; i++) {
				sTaskType[i] = taskType[i].toString();
			}
			
			Arrays.sort(sTaskType);
			
			String taskString = sTaskType[0]; 
			for (int i = 1; i < sTaskType.length; i++) {
				taskString += "/" + sTaskType[1];
			}
			
			res += taskString + ":";
		}
		
		return (res + ":" + localSynchronizationString()).intern();
	}
	
	protected String localSynchronizationString() {
		return ":" + getObject().getId().toString();
	}

	public abstract DocumentRepository<T> getRepository();
	
	@Override
	public T update(MongoUpdateInterface<T,F> mui) throws Exception {
		synchronized (saveSyncString()) { 
			load();
		
			mui.update(this);
			
			T object = getObject();
			if (object instanceof DatedDocument) {
				((DatedDocument) object).setUpdatedAt(new Date());
			}
			
			if (object instanceof IdentifiableDocument) {
				synchronized (globalSaveSyncString()) { // is this ok ?
					
					IdentifiableDocumentService<?,F> is = (IdentifiableDocumentService<?,F>)getService();
					
					for (IdentifierType type : is.identifierTypes()) {
						
						String identifier = ((IdentifiableDocument)object).getIdentifier(type);
						
						if (identifier != null) {
							if (((IdentifiableDocumentRepository<T>)getRepository()).existsSameIdentifier(object,type)) { 
								throw new StateConflictException("The identifier already exists.");
							}
						}					
					}

					getRepository().save(object);
				}
			} else {
				getRepository().save(object);
			}
		}
		
		return getObject();
	}
	
	public String saveSyncString() {
		return ("SYNC:SAVE:" + containerString() + ":" + getPrimaryId()).intern();
	}
	
	public String globalSaveSyncString() {
		return ("SYNC:SAVE:" + containerString()).intern();
	}

	protected void load() {
//		Optional<T> docOpt = getRepository().findByIdAndUserId(getPrimaryId(), new ObjectId(getCurrentUser().getId())); // not correct for loading regardless of user e.g. campaings
		Optional<T> docOpt = getRepository().findById(getPrimaryId());

		if (!docOpt.isPresent()) {
			return;
		}

		object = docOpt.get();
	}

	public abstract TaskDescription getActiveTask(TaskType type);
	
	public void checkIfActiveTask(List<TaskType> conflictingTasks) throws TaskConflictException {
		checkIfActiveTask(null, conflictingTasks);
	}
	
	public void checkIfActiveTask(TaskDescription parent, List<TaskType> conflictingTasks) throws TaskConflictException {
		
		for (TaskType t : TaskSpecification.getTaskTypesForContainerClass(this.getClass())) {
			if (conflictingTasks.contains(t)) {
				TaskDescription check = getActiveTask(t);
//				System.out.println(">> " + t + " " + (check != null ? check.getId() : check) + " " + (parent != null ? parent.getId() : parent));
//				if (check != null) {
//					System.out.println(":: " + isChildOrSameOf(check, parent));
//				}
				if (check != null && !isChildOrSameOf(check, parent)) {
					throw new TaskConflictException(TaskSpecification.getConflictMessage(t, getDescription()));
				}
			}
		}
	}
	
	private boolean isChildOrSameOf(TaskDescription check, TaskDescription parent) {
		if (parent == null) {
			return false;
		}

		if (check.getTopId().equals(parent.getTopId())) {
			return true;
		}

		
//		if (check.getId().equals(parent.getId())) {
//			return true;
//		}
//		
//		if (check.getRootId() != null) {
//			if (parent.getRootId() != null) {
//				return check.getRootId().equals(parent.getRootId());
//			} else {
//				return check.getRootId().equals(parent.getId());
//			}
//		}
//		
		return false;
	}

	@Override
	public UserPrincipal getObjectCreator() {
		return objectCreator;
	}

	public void setObjectCreator(UserPrincipal objectCreator) {
		this.objectCreator = objectCreator;
	}

}
