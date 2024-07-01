package ac.software.semantic.controller.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.ClassUtils;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLdApi;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;

import ac.software.semantic.config.AppConfiguration.JenaRDF2JSONLD;
import ac.software.semantic.model.AssigningContainer;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.IdentifiableDocument;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.Pagination;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.User;
import ac.software.semantic.model.base.ContentDocument;
import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.GroupedDocument;
import ac.software.semantic.model.base.InverseMemberDocument;
import ac.software.semantic.model.base.MemberDocument;
import ac.software.semantic.model.base.OrderedDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.state.PrepareState;
import ac.software.semantic.model.constants.type.FileType;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.payload.ListResult;
import ac.software.semantic.payload.request.InGroupRequest;
import ac.software.semantic.payload.request.MultipartFileUpdateRequest;
import ac.software.semantic.payload.request.UpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.IdentifierExistsResponse;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.payload.response.modifier.ResponseModifier;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ContainerService;
import ac.software.semantic.service.CreatableService;
import ac.software.semantic.service.EnclosedCreatableLookupService;
import ac.software.semantic.service.EnclosedCreatableService;
import ac.software.semantic.service.EnclosingLookupService;
import ac.software.semantic.service.EnclosingService;
import ac.software.semantic.service.ExecutingService;
import ac.software.semantic.service.FolderService;
import ac.software.semantic.service.IdentifiableDocumentService;
import ac.software.semantic.service.LookupService;
import ac.software.semantic.service.PublishingService;
import ac.software.semantic.service.ServiceProperties;
import ac.software.semantic.service.ServiceUtils;
import ac.software.semantic.service.TaskService;
import ac.software.semantic.service.TaskSpecification;
import ac.software.semantic.service.UserService;
import ac.software.semantic.service.ValidatingService;
import ac.software.semantic.service.container.AnnotationValidationContainer;
import ac.software.semantic.service.container.CreatableContainer;
import ac.software.semantic.service.container.EnclosedBaseContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.EnclosedContainer;
import ac.software.semantic.service.container.ExecutableContainer;
import ac.software.semantic.service.container.GroupingContainer;
import ac.software.semantic.service.container.IdentifierCachable;
import ac.software.semantic.service.container.IntermediatePublishableContainer;
import ac.software.semantic.service.container.InverseMemberContainer;
import ac.software.semantic.service.container.MemberContainer;
import ac.software.semantic.service.container.MultipleResponseContainer;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.StartableContainer;
import ac.software.semantic.service.container.TypeLookupContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.PreparableContainer;
import ac.software.semantic.service.container.PublishableContainer;
import ac.software.semantic.service.container.SchedulableContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.container.ValidatableContainer;
import ac.software.semantic.service.exception.ContainerNotFoundException;
import ac.software.semantic.service.exception.StateConflictException;
import ac.software.semantic.service.exception.TaskConflictException;
import ac.software.semantic.service.lookup.GroupLookupProperties;
import ac.software.semantic.service.lookup.LookupProperties;

@Service
public class APIUtils {

	Logger logger = LoggerFactory.getLogger(APIUtils.class);
	
    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;
    
	@Autowired
	private FolderService folderService;

	@Autowired
	private ServiceUtils serviceUtils;

	@Autowired
	private UserService userService;

	@Autowired
	private TaskService taskService;

	public <D extends SpecificationDocument, F extends Response> 
	ObjectContainer<D,F> exists(UserPrincipal currentUser, ObjectIdentifier objId, ContainerService<D,F> containerService) throws ContainerNotFoundException {
		ObjectContainer<D,F> oc = containerService.getContainer(currentUser, objId);
		if (oc == null ) {
			throw new ContainerNotFoundException(containerService.getContainerClass());
		}
		return oc;
		
	}

//	private <D extends SpecificationDocument> void checkIsExecuting(ObjectContainer<D> oc, boolean flag) throws TaskConflictException {
//    	if (oc instanceof ExecutableContainer) {
//    		boolean check = ((ExecutableContainer<?,?>)oc).isExecuting();
//    		if (!flag && check || flag && !check) {
//    			throw TaskConflictException.isExecuting((ExecutableContainer<?,?>)oc);
//    		}
//    	}
//	}
	
	private <D extends SpecificationDocument, F extends Response> void checkIsOwner(ObjectContainer<D,F> oc, boolean flag) throws TaskConflictException {
   		boolean check = oc.isCurrentUserOwner();
    	if (!flag && check || flag && !check) {
    		throw TaskConflictException.notOwner(oc);
    	}
	}
	
	private <D extends SpecificationDocument, F extends Response> void checkIsPublished(ObjectContainer<D,F> oc, boolean flag) throws TaskConflictException {
    	if (oc instanceof IntermediatePublishableContainer) {
    		boolean check = ((IntermediatePublishableContainer<D,F,?,?,?>)oc).isPublished();
    		if (!flag && check || flag && !check) {
    			throw TaskConflictException.isPublished((IntermediatePublishableContainer<D,F,?,?,?>)oc);
    		}
    	}
	}
	
	private <D extends SpecificationDocument, F extends Response> void checkIsScheduled(ObjectContainer<D,F> oc, boolean flag) throws TaskConflictException {
    	if (oc instanceof SchedulableContainer) {
    		boolean check = ((SchedulableContainer<D,F>)oc).isScheduled();
    		if (!flag && check || flag && !check) {
    			throw TaskConflictException.isScheduled((SchedulableContainer<D,F>)oc);
    		}
    	}
	}
	
	private <D extends SpecificationDocument, F extends Response> void checkIsCreated(ObjectContainer<D,F> oc, boolean flag) throws TaskConflictException {
    	if (oc instanceof CreatableContainer) {
    		boolean check = ((CreatableContainer<D,F,?,?>)oc).isCreated();
    		if (!flag && check || flag && !check) {
    			throw TaskConflictException.isCreated((CreatableContainer<D,F,?,?>)oc);
    		}
    	}
	}
	
	private <D extends SpecificationDocument, F extends Response> void checkIsStarted(ObjectContainer<D,F> oc, boolean flag) throws TaskConflictException {
    	if (oc instanceof StartableContainer) {
    		boolean check = ((StartableContainer<D,F>)oc).isStarted();
    		if (!flag && check || flag && !check) {
    			throw TaskConflictException.isStarted((StartableContainer<D,F>)oc);
    		}
    	}
	}
	
	private <D extends SpecificationDocument, F extends Response> void checkHasValidations(ObjectContainer<D,F> oc, boolean flag) throws TaskConflictException {
    	if (oc instanceof AnnotationValidationContainer) {
    		boolean check = ((AnnotationValidationContainer<?,?,?>)oc).hasValidations();
    		if (!flag && check || flag && !check) {
    			throw TaskConflictException.hasValidations((AnnotationValidationContainer<?,?,?>)oc);
    		}
    	}
	}
	
	public <D extends SpecificationDocument, F extends Response> 
	       APIResponse delete(UserPrincipal currentUser, ObjectIdentifier objId, ContainerService<D,F> containerService) {

		try {
			
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);

			checkIsOwner(oc, true);
	    	checkIsPublished(oc, false);
	    	checkIsScheduled(oc, false);
	    	checkIsCreated(oc, false);
	    	checkIsStarted(oc, false);
	    	checkHasValidations(oc, false);
	    	
			synchronized (oc.synchronizationString()) { // not correct -- synchronization is done on dataset
	    		oc.checkIfActiveTask(null, TaskSpecification.getTaskTypesForContainerClass(containerService.getContainerClass()));
	    		
	    		oc.delete(); // what if this fails ?

	    		return APIResponse.deleted(oc);
	    	}

		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass());
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex);		    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}
	
	public <D extends SpecificationDocument, F extends Response, U extends UpdateRequest, I extends EnclosingDocument> 
	    APIResponse delete(UserPrincipal currentUser, ObjectIdentifier objId, EnclosedCreatableService<D,F,U,I> containerService, ContainerService<I,?> enclosingService) {
	
		try {
			
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
	
			checkIsOwner(oc, true);
		 	checkIsPublished(oc, false);
		 	checkIsScheduled(oc, false);
		 	checkIsCreated(oc, false);
		 	checkIsStarted(oc, false);
		 	checkHasValidations(oc, false);
		 	
		 	EnclosedContainer<D,I> eoc = (EnclosedContainer<D,I>)oc;
		 	I enclosingObject = eoc.getEnclosingObject();
		 	
		 	ObjectContainer<I,?> dc = (ObjectContainer<I,?>)enclosingService.getContainer(currentUser, enclosingObject);
		 	
			List<Response> res = new ArrayList<>();
			
			synchronized (oc.synchronizationString()) { // not correct -- synchronization is done on dataset
		 		oc.checkIfActiveTask(null, TaskSpecification.getTaskTypesForContainerClass(containerService.getContainerClass()));
		 		
		 		boolean remove = oc.delete(); // what if this fails ?
		 		
		 		D ut = oc.getObject();
		 		
				if (remove && ut instanceof OrderedDocument) {
					int order = ((OrderedDocument) ut).getOrder();
				
					for (D doc : containerService.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {enclosingObject}), null, null).getList()) {
						ObjectContainer<D,F> xc = containerService.getContainer(currentUser, doc, enclosingObject);
						
						if (((OrderedDocument)doc).getOrder() > order) {
							
							xc.update(ouc -> {
								OrderedDocument od = (OrderedDocument)ouc.getObject();
								od.setOrder(od.getOrder() - 1);
							});
						}
						
						res.add(xc.asResponse());
					}
				} else {
					for (D doc : containerService.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {enclosingObject}), null, null).getList()) {
						ObjectContainer<D,F> xc = containerService.getContainer(currentUser, doc, enclosingObject);
						res.add(xc.asResponse());
					}
				}
				
				if (dc instanceof GroupingContainer) {
					((GroupingContainer<I>)dc).updateMaxGroup();
				}
				
		 		return APIResponse.deleted(oc, res);
		 	}
	 	
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass());
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex);		    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}
	
	public <D extends SpecificationDocument, F extends Response, U extends UpdateRequest, I extends EnclosingDocument, L extends LookupProperties> 
    APIResponse delete(UserPrincipal currentUser, ObjectIdentifier objId, EnclosedCreatableLookupService<D,F,U,I,L> containerService, ContainerService<I,?> enclosingService) {

	try {
		
		ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);

		checkIsOwner(oc, true);
	 	checkIsPublished(oc, false);
	 	checkIsScheduled(oc, false);
	 	checkIsCreated(oc, false);
	 	checkIsStarted(oc, false);
	 	checkHasValidations(oc, false);

	 	EnclosedContainer<D,I> eoc = (EnclosedContainer<D,I>)oc;
	 	I enclosingObject = eoc.getEnclosingObject();

//	 	TypeLookupContainer<D,F,L> loc = (TypeLookupContainer<D,F,L>)oc;
	 	
		List<Response> res = new ArrayList<>();
		
		synchronized (oc.synchronizationString()) { // not correct -- synchronization is done on dataset
	 		oc.checkIfActiveTask(null, TaskSpecification.getTaskTypesForContainerClass(containerService.getContainerClass()));
	 		
	 		oc.delete(); // what if this fails ?
	 		
	 		D ut = oc.getObject();
	 		
			if (ut instanceof OrderedDocument) {
				int order = ((OrderedDocument) ut).getOrder();
			
//				for (D doc : containerService.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {enclosingObject}), null, loc.buildTypeLookupPropetries(), null).getList()) {
				for (D doc : containerService.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {enclosingObject}), null, null, null).getList()) {
					ObjectContainer<D,F> xc = containerService.getContainer(currentUser, doc, enclosingObject);
					
					if (((OrderedDocument)doc).getOrder() > order) {
						
						xc.update(ouc -> {
							OrderedDocument od = (OrderedDocument)ouc.getObject();
							od.setOrder(od.getOrder() - 1);
						});
					}
					
					res.add(xc.asResponse());
				}
			} else {
//				for (D doc : containerService.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {enclosingObject}), null, loc.buildTypeLookupPropetries(), null).getList()) {
				for (D doc : containerService.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {enclosingObject}), null, null, null).getList()) {
					ObjectContainer<D,F> xc = containerService.getContainer(currentUser, doc, enclosingObject);
					res.add(xc.asResponse());
				}
			}
			
	 		return APIResponse.deleted(oc, res);
	 	}
 	
	} catch (ContainerNotFoundException ex) {
		return APIResponse.notFound(ex.getContainerClass());
	} catch (TaskConflictException ex) {
		return APIResponse.conflict(ex);		    	
	} catch (Exception ex) {
		ex.printStackTrace();
		return APIResponse.serverError(ex);
	}
}	
	
    public <D extends IdentifiableDocument, F extends Response> 
	    APIResponse identifierConflict(D object, IdentifierType type, IdentifiableDocumentService<D,F> containerService)  {

		try {
			
			IdentifierExistsResponse res = new IdentifierExistsResponse();
			
	    	if (!containerService.isValidIdentifier(object, type)) {
	    		res.setValid(false);
	    	} else {
	    		res.setValid(true);
	    	
				if (containerService.identifierConfict(object, type)) {
					res.setExists(true);
				} else {
					res.setExists(false);
				}
	    	}
				
			return APIResponse.SuccessResponse(null, res, HttpStatus.OK);

			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}				
	}

    public <D extends SpecificationDocument, F extends Response, U extends UpdateRequest> 
	    APIResponse cnew(UserPrincipal currentUser, U ur, CreatableService<D,F,U> containerService)  {
	
		try {
			
			D ut = containerService.create(currentUser, ur);
			ObjectContainer<D,F> uc = containerService.getContainer(currentUser, ut);
	
			return APIResponse.created(uc);
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass());			
		} catch (StateConflictException ex) {
			return APIResponse.conflict(ex);
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}				
	}
    
    public <D extends SpecificationDocument, F extends Response, U extends UpdateRequest, I extends EnclosingDocument> 
           APIResponse cnew(UserPrincipal currentUser, ObjectIdentifier objId, U ur, EnclosedCreatableService<D,F,U,I> containerService, ContainerService<I,?> enclosingService)  {

		try {
			I inside = null;
			
			if (objId != null) {
				ObjectContainer<I,?> ec = exists(currentUser, objId, enclosingService);
				inside = ec.getObject();
			}
			
			D ut = containerService.create(currentUser, inside, ur);
			ObjectContainer<D,F> uc = containerService.getContainer(currentUser, ut);
	
			if (ut instanceof OrderedDocument) {
				 
				if (ut instanceof GroupedDocument && containerService instanceof LookupService) {
					EnclosedCreatableLookupService lookupService = ((EnclosedCreatableLookupService)containerService);
				
					GroupLookupProperties lp = null;
					lp = (GroupLookupProperties)lookupService.createLookupProperties();
					lp.setGroup(((GroupedDocument)ut).getGroup());
					
					int size = lookupService.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {inside}), null, lp, null).getList().size();
					
					uc.update(ouc -> {
						OrderedDocument od = (OrderedDocument)ouc.getObject();
						od.setOrder(size - 1);
						
						GroupedDocument gd = (GroupedDocument)ouc.getObject();
						gd.setGroup(((GroupedDocument)ut).getGroup());
					});
					
				} else {
					int size = containerService.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {inside}), null, null).getList().size();
					
					uc.update(ouc -> {
						OrderedDocument od = (OrderedDocument)ouc.getObject();
						od.setOrder(size - 1);
					});
				}

			}
			
			return APIResponse.created(uc);
			
		} catch (StateConflictException ex) {
			return APIResponse.conflict(ex);			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass());			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}				
	}
    
    public <D extends SpecificationDocument, F extends Response, U extends MultipartFileUpdateRequest, I extends EnclosingDocument> 
    	APIResponse cnew(UserPrincipal currentUser, ObjectIdentifier objId, String json, MultipartFile file, EnclosedCreatableService<D,F,U,I> containerService, ContainerService<I,?> enclosingService)  {

		try {
			
			ObjectContainer<I,?> ec = exists(currentUser, objId, enclosingService);
			
			U ur = (U)buildMultipartFileUpdateRequest(json, file, containerService);
			
			D ut = containerService.create(currentUser, ec.getObject(), ur);
			ObjectContainer<D,F> uc = containerService.getContainer(currentUser, ut);

			if (ut instanceof OrderedDocument) {

				GroupLookupProperties lp = null; 
				if (ur instanceof InGroupRequest && containerService instanceof LookupService) {
					
					EnclosedCreatableLookupService lookupService = ((EnclosedCreatableLookupService)containerService);
					
					lp = (GroupLookupProperties)lookupService.createLookupProperties();
					lp.setGroup(((InGroupRequest)ur).getGroup());
					
					int size = lookupService.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {ec.getObject()}), null, lp, null).getList().size();
					
					uc.update(ouc -> {
						OrderedDocument od = (OrderedDocument)ouc.getObject();
						od.setOrder(size - 1);
						
						GroupedDocument gd = (GroupedDocument)ouc.getObject();
						gd.setGroup(((InGroupRequest)ur).getGroup());
					});
					
				} else {
					int size = containerService.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {ec.getObject()}), null, null).getList().size();
					
					uc.update(ouc -> {
						OrderedDocument od = (OrderedDocument)ouc.getObject();
						od.setOrder(size - 1);
					});
				}
				
			}
			
			return APIResponse.created(uc);
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass());			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}				
	}
    
	public <D extends SpecificationDocument, F extends Response> 
	       APIResponse get(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ContainerService<D,F> containerService)  {

		return get(currentUser, objId, containerService, null);
	}
	
	public <D extends SpecificationDocument, F extends Response, M extends ResponseModifier> 
    	APIResponse get(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ContainerService<D,F> containerService, M rm)  {

		try {
			
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
	 	
			return APIResponse.retrieved(oc, rm);
	 	
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass());	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}

	
	public <D  extends SpecificationDocument, F extends Response> 
	    APIResponse update(UserPrincipal currentUser, ObjectIdentifier objId, UpdateRequest ur, ContainerService<D,F> containerService) {

		try {
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
			
			checkIsOwner(oc, true);
			
			UpdatableContainer<D, F,UpdateRequest> uc = (UpdatableContainer<D,F,UpdateRequest>)oc;

			if (oc instanceof IdentifierCachable) {
				((IdentifierCachable<D>) oc).removeFromCache();
			}
			
		   	uc.update(ur);
		   	
		   	if (oc instanceof SchedulableContainer) {
		   		SchedulableContainer<?,?> sc = (SchedulableContainer<?,?>)oc;
		   		if (sc.isScheduled()) {
			   		sc.unschedule();
			   		sc.schedule();
		   		}
		   	}
			
			return APIResponse.updated(oc);
			
		} catch (StateConflictException ex) {
			return APIResponse.conflict(ex);	
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass());				
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}
	
	public <D  extends SpecificationDocument, F extends Response>
	     APIResponse update(UserPrincipal currentUser, ObjectIdentifier objId, String json, MultipartFile file, ContainerService<D,F> containerService)  {

    	try {
    		
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
			
	    	checkIsOwner(oc, true);

			UpdatableContainer<D, F,UpdateRequest> uc = (UpdatableContainer<D,F,UpdateRequest>)oc;
			
			uc.update(buildMultipartFileUpdateRequest(json, file, containerService));
			
			return APIResponse.updated(oc);			
	    	
    	} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
    	}
    }
	
	public <D  extends SpecificationDocument, F extends Response>
	       APIResponse prepare(UserPrincipal currentUser, ObjectIdentifier objId, ContainerService<D,F> containerService)  {

		try {
			
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
			
	    	checkIsOwner(oc, true);

			PreparableContainer rc = (PreparableContainer)oc;

			PrepareState status = rc.prepare();
			
			if (status == PrepareState.PREPARED) {
  				return APIResponse.ready(rc);
			} else if (status == PrepareState.PREPARING) {
				throw TaskConflictException.isPreparing(rc);
			} else if (status == PrepareState.NOT_PREPARED) {
				throw TaskConflictException.notPrepared(rc);
			} else {
				throw TaskConflictException.unknownPrepareState(rc);
	    	}
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass());				
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex);			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	    	
	}
	
	public <D  extends SpecificationDocument, F extends Response> 
		APIResponse validate(UserPrincipal currentUser, ObjectIdentifier objId, ValidatingService<D,F> containerService)  {
	
		try {
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
			
	    	checkIsOwner(oc, true);

			ValidatableContainer<?,?> vc = (ValidatableContainer<?,?>)oc;
			
			if (vc.getValidatorDocument() == null || vc.getValidatorDocument().size() == 0) {
				throw TaskConflictException.noValidator((ValidatableContainer<?,?>)oc);
			}

			TaskDescription tdescr = TaskSpecification.getTaskSpecification(vc.getValidateTask()).createTask(oc);
				
	    	if (tdescr != null) {
	    		taskService.call(tdescr);
	    		return APIResponse.acceptedToValidate(vc);
	    	} else {
	    		return APIResponse.serverError();
	    	}
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex);		    	
    	} catch (Exception ex) {
    		ex.printStackTrace();
    		return APIResponse.serverError(ex);
    	}
	}
	
	public <D  extends SpecificationDocument, F extends Response> 
	       APIResponse execute(UserPrincipal currentUser, ObjectIdentifier objId, ExecutingService<D,F> containerService)  {

		try {
			
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
			
			checkIsOwner(oc, true);
			
			ExecutableContainer<?,?,?,?> ec = (ExecutableContainer<?,?,?,?>)oc;
			
			if (oc instanceof PreparableContainer) {
				PreparableContainer rc = ((PreparableContainer)oc);
				
				PrepareState status = rc.isPrepared();
				
				if (status == PrepareState.PREPARING) {
					throw TaskConflictException.isPreparing(rc);
				} else if (status == PrepareState.NOT_PREPARED) {
					throw TaskConflictException.notPrepared(rc);
				} else if (status == PrepareState.UNKNOWN) {
					throw TaskConflictException.unknownPrepareState(rc);
		    	}
			}
			
			TaskDescription tdescr = TaskSpecification.getTaskSpecification(ec.getExecuteTask()).createTask(oc);
			
	    	if (tdescr != null) {
	    		taskService.call(tdescr);
	    		return APIResponse.acceptedToExecute(ec);
	    	} else {
	    		return APIResponse.serverError();
	    	}

		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass());				
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex);
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	
	}	
	
	public <D  extends SpecificationDocument, F extends Response> 
	       APIResponse stopExecution(UserPrincipal currentUser, ObjectIdentifier objId, ExecutingService<D,F> containerService)  {

		try {
			
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
			
			checkIsOwner(oc, true);
			
			ExecutableContainer<?,?,?,?> ec = (ExecutableContainer<?,?,?,?>)oc;

	    	synchronized (oc.synchronizationString()) {
	    		TaskDescription td =  oc.getActiveTask(ec.getExecuteTask());
	    		if (td != null) {
	    			if (taskService.requestStop(td.getId())) {
	    				return APIResponse.acceptedToStopExecution(ec);
	    			} else {
	    				return APIResponse.couldNotStopExecution(ec);
	    			}
	    		} else {
	    			ec.failExecution();

	    			return APIResponse.notExecuting(ec);
	    		}
	    	}
	    	
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass());				
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex);		    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	    	
	}

	public <D  extends SpecificationDocument, F extends Response> 
		APIResponse publish(UserPrincipal currentUser, ObjectIdentifier objId, PublishingService<D,F> containerService)  {
		return publish(currentUser, objId, containerService, null);
	}

	public <D  extends SpecificationDocument, F extends Response> 
	       APIResponse publish(UserPrincipal currentUser, ObjectIdentifier objId, PublishingService<D,F> containerService, Properties props)  {
		
		Class<? extends ObjectContainer<D,F>> containerClass = containerService.getContainerClass();
		
		synchronized (containerService.synchronizedString(objId.toHexString())) {
			ObjectContainer<D,F> oc = null;
			PublishableContainer<?,?,?,?,?> pc = null;
			
			try {
		    	oc = containerService.getContainer(currentUser, objId);
		    	
		    	pc = (PublishableContainer<?,?,?,?,?>)oc;
		    	
		    	if (oc == null) {
		    		return APIResponse.notFound(containerClass);
		    	}
		    	
		    	checkIsOwner(oc, true);
		    	
		    	if (pc.isPublished(props)) {
		    		throw TaskConflictException.alreadyPublished(pc);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.badRequest();
			}
			
			try {
				TaskDescription tdescr = TaskSpecification.getTaskSpecification(pc.isFailed() ? pc.getRepublishTask() : pc.getPublishTask()).createTask(oc, props);
				
		    	if (tdescr != null) {
		    		taskService.call(tdescr);
		    		return APIResponse.acceptedToPublish(pc);
		    	} else {
		    		return APIResponse.serverError();
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);				
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex);
			}
		}
	}    

	public <D  extends SpecificationDocument, F extends Response> 
		APIResponse unpublish(UserPrincipal currentUser, ObjectIdentifier objId, PublishingService<D,F> containerService)  {
		return unpublish(currentUser, objId, containerService, null);
	}

	public <D  extends SpecificationDocument, F extends Response> 
	       APIResponse unpublish(UserPrincipal currentUser, ObjectIdentifier objId, PublishingService<D,F> containerService, Properties props)  {

		Class<? extends ObjectContainer<D, F>> containerClass = containerService.getContainerClass();
		
		synchronized (containerService.synchronizedString(objId.toHexString())) {
			ObjectContainer<D,F> oc = null;
			PublishableContainer<?,?,?,?,?> pc = null;
			
			try {
		    	oc = containerService.getContainer(currentUser, objId);
		    	pc = (PublishableContainer<?,?,?,?,?>)oc;
		    	
		    	if (oc == null) {
		    		return APIResponse.notFound(containerClass);
		    	} 
		    	
		    	checkIsOwner(oc, true);
		    	
		    	if (!pc.isPublished(props) && !pc.isFailed(props)) {
		    		throw TaskConflictException.notPublished(pc);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.badRequest();
			}
			
			try {
		    	TaskDescription tdescr = TaskSpecification.getTaskSpecification(pc.getUnpublishTask()).createTask(oc, props);
				
		    	if (tdescr != null) {
		    		taskService.call(tdescr);
		    		return APIResponse.acceptedToUnpublish(pc);
		    	} else {
		    		return APIResponse.serverError();
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);				
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex);
			}
		}
	}   	

	public <D  extends SpecificationDocument, F extends Response> 
 	    APIResponse republish(UserPrincipal currentUser, ObjectIdentifier objId, PublishingService<D,F> containerService)  {
		return republish(currentUser, objId, containerService, null);
	}

	public <D  extends SpecificationDocument, F extends Response> 
	       APIResponse republish(UserPrincipal currentUser, ObjectIdentifier objId, PublishingService<D,F> containerService, Properties props)  {

		Class<? extends ObjectContainer<D,F>> containerClass = containerService.getContainerClass();
		
		synchronized (containerService.synchronizedString(objId.toHexString())) {
			ObjectContainer<D,F> oc = null;
			PublishableContainer<?,?,?,?,?> pc = null;
			
			try {
		    	oc = containerService.getContainer(currentUser, objId);
		    	pc = (PublishableContainer<?,?,?,?,?>)oc;
		    	
		    	if (oc == null) {
		    		return APIResponse.notFound(containerClass);
		    	} 
		    	
		    	checkIsOwner(oc, true);

		    	if (!pc.isPublished(props)) {
		    		throw TaskConflictException.notPublished(pc);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.badRequest();
			}
			
			try {
				if (props == null) {
					props = new Properties();
				}
				
				if (props.get(ServiceProperties.TRIPLE_STORE) == null) {
					props.put(ServiceProperties.TRIPLE_STORE, pc.getDatasetTripleStoreVirtuosoConfiguration());
				}
				
		    	TaskDescription tdescr = TaskSpecification.getTaskSpecification(pc.getRepublishTask()).createTask(oc, props);
				
		    	if (tdescr != null) {
		    		taskService.call(tdescr);
		    		return APIResponse.acceptedToRepublish(pc);
		    	} else {
		    		return APIResponse.serverError();
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);				
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex);
			}
		}
	}   	
	
 	public <D  extends SpecificationDocument, F extends Response> 
 	       APIResponse clearExecution(UserPrincipal currentUser, ObjectIdentifier objId, ExecutingService<D,F> containerService)  {
		
 		Class<? extends ObjectContainer<D,F>> containerClass = containerService.getContainerClass();
 		
 		ObjectContainer<?,?> oc = null;
 		ExecutableContainer<?,?,?,?> ec;
		try {
			oc = containerService.getContainer(currentUser, objId);
			ec = (ExecutableContainer<?,?,?,?>)oc;
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
	    	
	    	checkIsOwner(oc, true);

		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
		
		try {
			synchronized (oc.synchronizationString()) {
				boolean ok = ec.clearExecution();
				if (ok) {
					return APIResponse.executionDeleted(oc);
				} else { // incorrect... should work out actual errors
					return APIResponse.executionDeleteError(oc); 
				}
			}			
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex);				
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
 	}	
 	
 	public <D  extends SpecificationDocument, F extends Response, I extends EnclosingDocument>
 		APIResponse create(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ContainerService<D,F> containerService, ContainerService<I,?> enclosingService)  {
		
		synchronized (containerService.synchronizedString(objId.toHexString())) {
			
			try {
				ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
				CreatableContainer<D,F,?,?> cc = (CreatableContainer<D,F,?,?>)oc;
		    	
		    	I enclosingObject = ((EnclosedBaseContainer<D,F,I>)oc).getEnclosingObject();
		    	
	    		ObjectContainer<I,?> eoc = enclosingService.getContainer(currentUser, enclosingObject);
	    		PublishableContainer<?,?,?,?,?> epc = (PublishableContainer<?,?,?,?,?>)eoc;
	    		
		    	if (eoc == null) {
		    		return APIResponse.notFound(enclosingService.getContainerClass());
		    	} else if (!epc.isPublished()) {
		    		throw TaskConflictException.notPublished(epc);
		    	} else if (cc.isCreated()) {
		    		throw TaskConflictException.alreadyCreated(cc);
		    	}

	   			TaskDescription tdescr = TaskSpecification.getTaskSpecification(cc.getCreateTask()).createTask(oc);
	
		    	if (tdescr != null) {
		   			taskService.call(tdescr);
		   			return APIResponse.acceptedToCreate(cc);
		    	} else {
		    		return APIResponse.serverError();
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex);
			}
    	}
	}
 	
 	public <D  extends SpecificationDocument, F extends Response, I extends EnclosingDocument>
		APIResponse recreate(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ContainerService<D,F> containerService, ContainerService<I,?> enclosingService)  {
	
	synchronized (containerService.synchronizedString(objId.toHexString())) {
		
		try {
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
			CreatableContainer<D,F,?,?> cc = (CreatableContainer<D,F,?,?>)oc;
	    	
	    	I enclosingObject = ((EnclosedBaseContainer<D,F,I>)oc).getEnclosingObject();
	    	
    		ObjectContainer<I,?> eoc = enclosingService.getContainer(currentUser, enclosingObject);
    		PublishableContainer<?,?,?,?,?> epc = (PublishableContainer<?,?,?,?,?>)eoc;
    		
	    	if (eoc == null) {
	    		return APIResponse.notFound(enclosingService.getContainerClass());
	    	} else if (!epc.isPublished()) {
	    		throw TaskConflictException.notPublished(epc);
	    	} else if (!cc.isCreated()) {
	    		throw TaskConflictException.notCreated(cc);
	    	}

   			TaskDescription tdescr = TaskSpecification.getTaskSpecification(cc.getRecreateTask()).createTask(oc);

	    	if (tdescr != null) {
	   			taskService.call(tdescr);
	   			return APIResponse.acceptedToRecreate(cc);
	    	} else {
	    		return APIResponse.serverError();
	    	}
		} catch (TaskConflictException ex) {
			return APIResponse.conflict(ex);
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}
}
 	
 	public <D  extends SpecificationDocument, F extends Response, I extends EnclosingDocument>
		APIResponse stopCreate(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ContainerService<D,F> containerService)  {

 		synchronized (containerService.synchronizedString(objId.toHexString())) {

			try {
				ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
				
		    	checkIsOwner(oc, true);

				CreatableContainer<D,F,?,?> cc = (CreatableContainer<D,F,?,?>)oc;

		    	synchronized (oc.synchronizationString()) {
		    		TaskDescription td =  oc.getActiveTask(cc.getCreateTask());
		    		if (td != null) {
		    			if (taskService.requestStop(td.getId())) {
		    				return APIResponse.acceptedToStopCreating(cc);
		    			} else {
		    				return APIResponse.couldNotStopCreating(cc);
		    			}
		    		} else {
		    			cc.failCreating();
	
		    			return APIResponse.notCreating(cc);
		    		}
		    	}

			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex);
			}
		}
	}  
 	
 	public <D  extends SpecificationDocument, F extends Response>
 	APIResponse destroy(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ContainerService<D,F> containerService)  {
	
		synchronized (containerService.synchronizedString(objId.toHexString())) {
			
			try {
				ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
				
		    	checkIsOwner(oc, true);

				CreatableContainer<D,F,?,?> cc = (CreatableContainer<D,F,?,?>)oc;
					
			    if (!cc.isCreated()) {
			    	throw TaskConflictException.notCreated(cc);
			    }
			
				TaskDescription tdescr = TaskSpecification.getTaskSpecification(cc.getDestroyTask()).createTask(oc);
	
		    	if (tdescr != null) {
		   			taskService.call(tdescr);
		   			
		   			return APIResponse.acceptedToDestroy(cc);
		    	} else {
		    		return APIResponse.serverError();
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex);
			}
	  	}
	} 

 	public <D extends MemberDocument, F extends Response, G extends SpecificationDocument> 
 		APIResponse addMember(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ObjectIdentifier memberObjId, ContainerService<D,F> containerService, ContainerService<G,?> memberContainerService)  {
		return addMember(currentUser, objId, memberObjId, containerService, memberContainerService, null);
 	}

 	public <D extends MemberDocument, F extends Response, G extends SpecificationDocument, M extends ResponseModifier> 
 		APIResponse addMember(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ObjectIdentifier memberObjId, ContainerService<D,F> containerService, ContainerService<G,?> memberContainerService, M rm)  {
		
 		Class<? extends ObjectContainer<D,F>> containerClass = containerService.getContainerClass();
 		Class<? extends ObjectContainer<G,?>> memberContainerClass = memberContainerService.getContainerClass();
		
 		ObjectContainer<D,F> oc = null;
 		ObjectContainer<G,?> memberOc = null;
		
 		try {
			oc = containerService.getContainer(currentUser, objId);
			
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
	    	
	    	memberOc = memberContainerService.getContainer(currentUser, memberObjId);
			
	    	if (memberOc == null) {
	    		return APIResponse.notFound(memberContainerClass);
	    	}

	    	MemberContainer<D,?,G> mc = (MemberContainer<D,?,G>)oc;
	    	
	    	if (mc.hasMember(memberOc.getObject())) {
	    		return APIResponse.alreadyMember(mc, memberOc);
	    	}
	    	
	    	mc.addMember(memberOc.getObject());
	    	
	    	return APIResponse.memberAdded((MemberContainer<D,F,?> & MultipleResponseContainer<D,F,M>)mc, memberOc, rm);

		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	} 
	
 	public <M extends SpecificationDocument, F extends Response, D extends InverseMemberDocument<M>> 
		APIResponse addAsMember(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ObjectIdentifier memberObjId, ContainerService<D,F> containerService, ContainerService<M,?> targetContainerService)  {
	
		Class<? extends ObjectContainer<D,F>> containerClass = containerService.getContainerClass();
		Class<? extends ObjectContainer<M,?>> targetContainerClass = targetContainerService.getContainerClass();
	
		ObjectContainer<D,F> oc = null;
		ObjectContainer<M,?> targetOc = null;
	
		try {
			oc = containerService.getContainer(currentUser, objId);
			
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
	    	
	    	targetOc = targetContainerService.getContainer(currentUser, memberObjId);
			
	    	if (targetOc == null) {
	    		return APIResponse.notFound(targetContainerClass);
	    	}
	
	    	InverseMemberContainer<D,?,M> mc = (InverseMemberContainer<D,?,M>)oc;
	    	
	    	if (mc.isMemberOf(targetOc.getObject())) {
	    		return APIResponse.alreadyMember(targetOc, mc);
	    	}
	    	
	    	mc.addTo(targetOc.getObject());
	    	
	    	return APIResponse.memberAdded(oc);
	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	} 
 	
 	public <M extends SpecificationDocument, F extends Response, D extends InverseMemberDocument<M>> 
		APIResponse removeFromMember(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ObjectIdentifier memberObjId, ContainerService<D,F> containerService, ContainerService<M,?> targetContainerService)  {
	
		Class<? extends ObjectContainer<D,F>> containerClass = containerService.getContainerClass();
		Class<? extends ObjectContainer<M,?>> targetContainerClass = targetContainerService.getContainerClass();
	
		ObjectContainer<D,F> oc = null;
		ObjectContainer<M,?> targetOc = null;
		
		try {
			oc = containerService.getContainer(currentUser, objId);
			
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
	    	
	    	targetOc = targetContainerService.getContainer(currentUser, memberObjId);
			
	    	if (targetOc == null) {
	    		return APIResponse.notFound(targetContainerClass);
	    	}
	
	    	InverseMemberContainer<D,?,M> mc = (InverseMemberContainer<D,?,M>)oc;
	    	
	    	if (!mc.isMemberOf(targetOc.getObject())) {
	    		return APIResponse.notMember(targetOc, mc);
	    	}
	    	
	    	mc.removeFrom(targetOc.getObject());
	    	
	    	return APIResponse.memberRemoved(oc);
	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}
 	
 	public <D extends MemberDocument, F extends Response, M extends SpecificationDocument> 
		APIResponse removeMember(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ObjectIdentifier memberObjId, ContainerService<D,F> containerService, ContainerService<M,?> memberContainerService)  {
	
		Class<? extends ObjectContainer<D,F>> containerClass = containerService.getContainerClass();
		Class<? extends ObjectContainer<M,?>> memberContainerClass = memberContainerService.getContainerClass();
	
		ObjectContainer<D,F> oc = null;
		ObjectContainer<M,?> memberOc = null;
		
		try {
			oc = containerService.getContainer(currentUser, objId);
			
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
	    	
	    	memberOc = memberContainerService.getContainer(currentUser, memberObjId);
			
	    	if (memberOc == null) {
	    		return APIResponse.notFound(memberContainerClass);
	    	}
	
	    	MemberContainer<D,?,M> mc = (MemberContainer<D,?,M>)oc;
	    	
	    	if (!mc.hasMember(memberOc.getObject())) {
	    		return APIResponse.notMember(mc, memberOc);
	    	}
	    	
	    	mc.removeMember(memberOc.getObject());
	    	
	    	return APIResponse.memberRemoved(memberOc);
	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}
 	
	public <D extends MemberDocument, F extends Response, B extends SpecificationDocument, C extends Response, M extends ResponseModifier> 
		APIResponse getMembers(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ContainerService<D,F> containerService, ContainerService<B,C> memberContainerService, M mr)  {
	
		Class<? extends ObjectContainer<D,F>> containerClass = containerService.getContainerClass();
	
		ObjectContainer<D,F> oc = null;
	
		try {
			oc = containerService.getContainer(currentUser, objId);
			
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
	    	
	    	MemberContainer<D,F,B> mc = (MemberContainer<D,F,B>)oc;
	    	
			List<C> res = new ArrayList<>();
			Pagination pg = null;
	
			List<ObjectId> members = mc.getObject().getMemberIds(memberContainerService.getSpecificationDocumentClass());
			
	    	if (members != null) {
				for (ObjectId doc : members) {
					ObjectContainer<B,C> memberOc = memberContainerService.getContainer(null, new SimpleObjectIdentifier(doc));
					
					if (mr == null) {
						res.add(memberOc.asResponse());
					} else {
						res.add(((MultipleResponseContainer<B,C,M>)memberOc).asResponse(mr));
					}
					
//					pg = list.getPagination();
				}
			}
			
	    	return APIResponse.result(new ListResult<C>(res, pg));
	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	} 

 	public <D extends SpecificationDocument, F extends Response, A extends SpecificationDocument, M extends ResponseModifier> 
		APIResponse assign(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ObjectIdentifier assignmentObjId, ObjectIdentifier userObjId, ContainerService<D,F> containerService, ContainerService<A,?> assignmentContainerService, M rm)  {
	
		Class<? extends ObjectContainer<D,F>> containerClass = containerService.getContainerClass();
		Class<? extends ObjectContainer<A,?>> assignmentContainerClass = assignmentContainerService.getContainerClass();
		Class<? extends ObjectContainer<User,?>> userContainerClass = userService.getContainerClass();
	
		AssigningContainer<D,F,A> oc = null;
		ObjectContainer<A,?> assignmentOc = null;
		ObjectContainer<User,?> userOc = null;
		
		try {
			oc = (AssigningContainer<D,F,A>)containerService.getContainer(currentUser, objId);
			
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
	    	
	    	assignmentOc = assignmentContainerService.getContainer(currentUser, assignmentObjId);
			
	    	if (assignmentOc == null) {
	    		return APIResponse.notFound(assignmentContainerClass);
	    	}
	    	
	    	userOc = userService.getContainer(currentUser, userObjId);

	    	if (userOc == null) {
	    		return APIResponse.notFound(userContainerClass);
	    	}
	
	    	if (oc.isAssigned(assignmentOc.getObject(), userOc.getObject())) {
	    		return APIResponse.alreadyAssigned(assignmentOc, userOc);
	    	}
	    	
	    	oc.assign(assignmentOc.getObject(), userOc.getObject());
	    	
	    	return APIResponse.assigned(assignmentOc, userOc, rm);
	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}

 	public <D extends MemberDocument, F extends Response, A extends SpecificationDocument, B extends Response, M extends ResponseModifier> 
		APIResponse assignAll(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ObjectIdentifier userObjId, ContainerService<D,F> containerService, ContainerService<A,B> assignmentContainerService, M rm)  {
	
		Class<? extends ObjectContainer<D,F>> containerClass = containerService.getContainerClass();
//		Class<? extends ObjectContainer<A,?>> assignmentContainerClass = assignmentContainerService.getContainerClass();
		Class<? extends ObjectContainer<User,?>> userContainerClass = userService.getContainerClass();
	
		AssigningContainer<D,F,A> oc = null;
		ObjectContainer<A,B> assignmentOc = null;
		ObjectContainer<User,?> userOc = null;
		
		try {
			oc = (AssigningContainer<D,F,A>)containerService.getContainer(currentUser, objId);
			
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
	    	
	    	userOc = userService.getContainer(currentUser, userObjId);

	    	if (userOc == null) {
	    		return APIResponse.notFound(userContainerClass);
	    	}

	    	MemberContainer<D,F,?> memberContainer = (MemberContainer<D,F,?>)oc;
	    	
	    	List<ObjectContainer<?,B>> res = new ArrayList<>();
	    	
	    	List<ObjectId> memberIds = memberContainer.getObject().getMemberIds(assignmentContainerService.getSpecificationDocumentClass());
	    	
	    	if (memberIds != null) {
	    		for (ObjectId memberId : memberIds) {
	    	    	assignmentOc = assignmentContainerService.getContainer(currentUser, new SimpleObjectIdentifier(memberId));
	    			
	    	    	if (assignmentOc != null && !oc.isAssigned(assignmentOc.getObject(), userOc.getObject())) {
	    		    	oc.assign(assignmentOc.getObject(), userOc.getObject());
    		    		res.add(assignmentOc);	
	    	    	}
	    		}
	    	}
	    	
	    	return APIResponse.assigned(res, userOc, rm);
	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}

 	public <D extends SpecificationDocument, F extends Response, A extends SpecificationDocument, M extends ResponseModifier> 
		APIResponse unassign(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ObjectIdentifier assignmentObjId, ObjectIdentifier userObjId, ContainerService<D,F> containerService, ContainerService<A,?> assignmentContainerService, M rm)  {
	
		Class<? extends ObjectContainer<D,F>> containerClass = containerService.getContainerClass();
		Class<? extends ObjectContainer<A,?>> assignmentContainerClass = assignmentContainerService.getContainerClass();
		Class<? extends ObjectContainer<User,?>> userContainerClass = userService.getContainerClass();
	
		AssigningContainer<D,F,A> oc = null;
		ObjectContainer<A,?> assignmentOc = null;
		ObjectContainer<User,?> userOc = null;
		
		try {
			oc = (AssigningContainer<D,F,A>)containerService.getContainer(currentUser, objId);
			
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
	    	
	    	assignmentOc = assignmentContainerService.getContainer(currentUser, assignmentObjId);
			
	    	if (assignmentOc == null) {
	    		return APIResponse.notFound(assignmentContainerClass);
	    	}
	    	
	    	userOc = userService.getContainer(currentUser, userObjId);

	    	if (userOc == null) {
	    		return APIResponse.notFound(userContainerClass);
	    	}
	
	    	if (!oc.isAssigned(assignmentOc.getObject(), userOc.getObject())) {
	    		return APIResponse.notAssigned(assignmentOc, userOc);
	    	}
	    	
	    	oc.unassign(assignmentOc.getObject(), userOc.getObject());
	    	
	    	return APIResponse.unassigned(assignmentOc, userOc, rm);
	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}
 	
 	public <D extends MemberDocument, F extends Response, A extends SpecificationDocument, B extends Response> 
		APIResponse unassignAll(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ObjectIdentifier userObjId, ContainerService<D,F> containerService, ContainerService<A,B> assignmentContainerService)  {
	
		Class<? extends ObjectContainer<D,F>> containerClass = containerService.getContainerClass();
		Class<? extends ObjectContainer<User,?>> userContainerClass = userService.getContainerClass();
	
		AssigningContainer<D,F,A> oc = null;
		ObjectContainer<User,?> userOc = null;
		
		try {
			oc = (AssigningContainer<D,F,A>)containerService.getContainer(currentUser, objId);
			
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
	    	
	    	userOc = userService.getContainer(currentUser, userObjId);

	    	if (userOc == null) {
	    		return APIResponse.notFound(userContainerClass);
	    	}

	    	int count = oc.unassignAll(userOc.getObject());
	    	
	    	return APIResponse.unassigned(count, assignmentContainerService.getContainerClass(), userOc);
	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}
 	
 	public <D extends SpecificationDocument, F extends Response, A extends SpecificationDocument, B extends Response, M extends ResponseModifier> 
		APIResponse getAllAssigned(@CurrentUser UserPrincipal currentUser, ObjectIdentifier objId, ObjectIdentifier userObjId, ContainerService<D,F> containerService, ContainerService<A,B> assignmentContainerService, M rm)  {
	
		Class<? extends ObjectContainer<D,F>> containerClass = containerService.getContainerClass();
//		Class<? extends ObjectContainer<A,B>> assignmentContainerClass = assignmentContainerService.getContainerClass();
		Class<? extends ObjectContainer<User,?>> userContainerClass = userService.getContainerClass();
	
		AssigningContainer<D,F,A> oc = null;
		ObjectContainer<User,?> userOc = null;
		
		try {
			oc = (AssigningContainer<D,F,A>)containerService.getContainer(currentUser, objId);
			
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
	    	
	    	userOc = userService.getContainer(currentUser, userObjId);

	    	if (userOc == null) {
	    		return APIResponse.notFound(userContainerClass);
	    	}

	    	List<A> assignments = oc.getAssigned(userOc.getObject());
	    	
	    	List<B> res = new ArrayList<>();
	    	
	    	for (A assignment : assignments) {
	    		ObjectContainer<A,B> assignmentOc = assignmentContainerService.getContainer(currentUser, new SimpleObjectIdentifier(assignment.getId()));
	    		
	    		res.add(toResponse(assignmentOc, rm));
	    	}
	    	
	    	return APIResponse.result(new ListResult<B>(res, null));
	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}
 	
 	public <D extends ContentDocument, F extends Response> 
 		APIResponse getContent(UserPrincipal currentUser, ObjectIdentifier objId, ContainerService<D,F> containerService)  {

		try {
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
	    	
	    	return APIResponse.contentRetrieved(oc);
	    	
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass());	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
 	}
 	
 	public <D extends OrderedDocument, F extends Response, I extends EnclosingDocument> 
 		APIResponse changeOrder(UserPrincipal currentUser, ObjectIdentifier objId, int step, EnclosingService<D,F,I> containerService, ContainerService<I,?> enclosingService)  {
 		
 		try {
 			
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
			
	    	checkIsOwner(oc, true);
	    	
	    	D doc = oc.getObject();
	    	
	    	int currentOrder = oc.getObject().getOrder();
	    	int newOrder = currentOrder + step;
	    	
	    	if (newOrder < 0) {
	    		throw new Exception();
	    	}
	    	
	    	ObjectContainer<D,F> swapOc = null;
	    	
	    	I dataset = ((EnclosedObjectContainer<D,F,I>)oc).getEnclosingObject();
	    	
	    	List<D> docList;
	    	
			GroupLookupProperties lp = null; 
			if (doc instanceof GroupedDocument && containerService instanceof LookupService) {
					
				EnclosedCreatableLookupService lookupService = ((EnclosedCreatableLookupService)containerService);
				
				lp = (GroupLookupProperties)lookupService.createLookupProperties();
				lp.setGroup(((GroupedDocument)doc).getGroup());
				
				docList = lookupService.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {dataset}), null, lp, null).getList();
				
			} else {
				docList = containerService.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {dataset}), null, null).getList();
			}
	    	
	    	for (D edoc : docList) { // TODO: replace with search by order
	    		if (edoc.getOrder() == newOrder) {
	    			swapOc = containerService.getContainer(currentUser, edoc);
	    		}
	    	}
	    	
	    	if (swapOc != null) {
		    	oc.update(ioc -> {
		    		D idoc = ioc.getObject();
		    		idoc.setOrder(newOrder);
		    	});
	
		    	swapOc.update(ioc -> {
		    		D idoc = ioc.getObject();
		    		idoc.setOrder(currentOrder);
		    	});
		    	
		    	return APIResponse.result(getAllByUser(currentUser, null, Arrays.asList(new ObjectId[] {dataset.getId()}), containerService, enclosingService));
		    	
	    	} else {
	    		throw new Exception();
	    	}
	    	
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass());	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
 	}

 	public <D extends OrderedDocument & GroupedDocument, F extends Response, I extends EnclosingDocument> 
		APIResponse changeGroup(UserPrincipal currentUser, ObjectIdentifier objId, int newGroup, EnclosingService<D,F,I> containerService, ContainerService<I,?> enclosingService)  {
		
		try {
			
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
			
	    	checkIsOwner(oc, true);

	    	EnclosedObjectContainer<D,F,I> eoc = (EnclosedObjectContainer<D,F,I>)oc;
		 	I enclosingObject = eoc.getEnclosingObject();
		 	
		 	List<F> res = new ArrayList<>();

		 	ObjectContainer<I,?> dc = (ObjectContainer<I,?>)enclosingService.getContainer(currentUser, enclosingObject);

		 	int maxGroup = 0;

			synchronized (dc.synchronizationString()) { // not correct -- synchronization is done on dataset

		 		D ut = oc.getObject();
		 		
				int order = ut.getOrder();
				int group = ut.getGroup();
				
				int maxOrder = -1;
				
				for (D doc : containerService.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {enclosingObject}), null, null).getList()) {
					ObjectContainer<D,F> xc = containerService.getContainer(currentUser, doc, enclosingObject);

					int docOrder = doc.getOrder();
					int docGroup = doc.getGroup();
					
					if (docGroup != group && docGroup != newGroup) {
						res.add(xc.asResponse());
						
						maxGroup = Math.max(maxGroup, docGroup);
						
					} else if (docGroup == group) {
						if (docOrder > order) {
							
							xc.update(ouc -> {
								OrderedDocument od = (OrderedDocument)ouc.getObject();
								od.setOrder(od.getOrder() - 1);
							});
						}
						
						if (docOrder != order) {
							res.add(xc.asResponse());
							
							maxGroup = Math.max(maxGroup, docGroup);
						}
					} else if (docGroup == newGroup) {
						res.add(xc.asResponse());

						maxOrder = Math.max(maxOrder, docOrder);
					}
				}
				
				maxGroup = Math.max(maxGroup, newGroup);

				final int newOrder = maxOrder + 1;
		    	oc.update(ioc -> {
		    		D doc = ioc.getObject();
					doc.setGroup(newGroup);
					doc.setOrder(newOrder);
		    	});
		    	
		    	res.add(oc.asResponse());
		    	
		    	((GroupingContainer<I>)dc).updateMaxGroup();
	    	}
			
			return APIResponse.result(res);
	    	
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass());	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
	}

	public <D  extends SpecificationDocument, F extends Response> 
		ResponseEntity<?> previewLastExecution(UserPrincipal currentUser, ObjectIdentifier objId, int shard, int offset, ContainerService<D,F> containerService)  {

		try {
			
			ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
			ExecutableContainer<?,?,?,Dataset> ec = (ExecutableContainer<?,?,?,Dataset>)oc;
			
			ExecuteState es = ec.getExecuteState();
	
			if (es.getExecuteState() != MappingState.EXECUTED) {
				return APIResponse.noContent().toResponseEntity();
			}
	
			File file = folderService.getExecutionTrigFile(ec, es, shard);
			if (file == null && shard == 0) {
				return ResponseEntity.ok("" + System.getProperty("line.separator") + 0 + "/" + 0 + "/" + -1 + System.getProperty("line.separator"));
			}
			
			String fileName = getFileName(file);
			
			FileRead fr = FileUtils.readFileLines(Paths.get(file.getAbsolutePath()), shard, offset);
			while (es.getExecuteShards() > shard + 1 && fr.getLines() < FileUtils.maxLines) {
				file = folderService.getExecutionTrigFile(ec, es, ++shard);
				if (file == null) {
					break;
				} else {
					FileRead fr2 = FileUtils.readFileLines(Paths.get(file.getAbsolutePath()), shard, 0);
					fr = fr.merge(fr2);
				}
					
			}
			return ResponseEntity.ok(fileName + System.getProperty("line.separator") + fr.getLines() + "/" + fr.getShard() + "/" + fr.getNextLine() + System.getProperty("line.separator") + fr.getContent());
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}	
	
	private String getFileName(File file) { 
		String fileName = file.getName();
		fileName = fileName.substring(0, fileName.lastIndexOf("."));
		int p = fileName.lastIndexOf("_#");
		if (p != -1) {
			fileName = fileName.substring(0, p);
		}
		
		return fileName;
	}
	
	public <D  extends SpecificationDocument, F extends Response> 
    ResponseEntity<?> previewPublishedExecution(UserPrincipal currentUser, ObjectIdentifier objId, int shard, int offset, ExecutingService<D,F> containerService)  {

	try {
		
		ObjectContainer<D,F> oc = exists(currentUser, objId, containerService);
		
		ExecutableContainer<?,?,ExecuteState,Dataset> ec = (ExecutableContainer)oc;
		IntermediatePublishableContainer<?,?,ExecuteState,PublishState<ExecuteState>,Dataset> pc = (IntermediatePublishableContainer)oc;
		
		ProcessStateContainer psv = pc.getCurrentPublishState();
		if (psv == null) {
			return APIResponse.noContent().toResponseEntity();			
		}
		
		PublishState<ExecuteState> ps = (PublishState)psv.getProcessState();
		ExecuteState pes = ps.getExecute();
		
		if (!pes.getDatabaseConfigurationId().equals(fileSystemConfiguration.getId())) {
			return APIResponse.noContent().toResponseEntity();
		}
		
		if (pes.getExecuteState() != MappingState.EXECUTED) {
			return APIResponse.noContent().toResponseEntity();
		}

		
		File file = folderService.getExecutionTrigFile(ec, pes, shard);
		if (file == null && shard == 0) {
			return ResponseEntity.ok("" + System.getProperty("line.separator") + 0 + "/" + 0 + "/" + -1 + System.getProperty("line.separator"));
		}
		
		String fileName = getFileName(file);
		
		FileRead fr = FileUtils.readFileLines(Paths.get(file.getAbsolutePath()), shard, offset);
		while (pes.getExecuteShards() > shard + 1 && fr.getLines() < FileUtils.maxLines) {
			file = folderService.getExecutionTrigFile(ec, pes, ++shard);
			if (file == null) {
				break;
			} else {
				FileRead fr2 = FileUtils.readFileLines(Paths.get(file.getAbsolutePath()), shard, 0);
				fr = fr.merge(fr2);
			}
				
		}
		
		
		return ResponseEntity.ok(fileName + System.getProperty("line.separator") + fr.getLines() + "/" + fr.getShard() + "/" + fr.getNextLine() + System.getProperty("line.separator") + fr.getContent());
		
	} catch (ContainerNotFoundException ex) {
		return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();			
	} catch (Exception ex) {
		ex.printStackTrace();
		return APIResponse.serverError(ex).toResponseEntity();
	}
}	
	
	public <D  extends SpecificationDocument, F extends Response> 
	       ResponseEntity<StreamingResponseBody> downloadLastExecution(UserPrincipal currentUser, ObjectIdentifier objId, ExecutingService<D,F> containerService, FileType fileType)  {

//		Class<? extends ObjectContainer> containerClass = containerService.getContainerClass();
		
		ExecutableContainer<?,?,MappingExecuteState,Dataset> ec = null;
		try {
			ec = (ExecutableContainer<?,?,MappingExecuteState,Dataset>)containerService.getContainer(currentUser, objId);
	    	if (ec == null) {
	    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}	
		
		try {
			MappingExecuteState es = ec.getExecuteState();  // TODO: Should check that it is executed
					
			File file = folderService.getExecutionFile(ec, es, fileType);
			
			if (file == null) {
				file = serviceUtils.zipExecution(ec, es, es.getExecuteShards() == 0 ? 1 : es.getExecuteShards());
			}	

			return downloadFile(file.getAbsolutePath());

		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
	}    
	
	public <D  extends SpecificationDocument, F extends Response> 
	       ResponseEntity<StreamingResponseBody> downloadPublishedExecution(UserPrincipal currentUser, ObjectIdentifier objId, ExecutingService<D,F> containerService)  {
		
//		Class<? extends ObjectContainer> containerClass = containerService.getContainerClass();
		
		ObjectContainer<D,F> oc = null;
		try {
			oc = containerService.getContainer(currentUser, objId);
			if (oc == null) {
	    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}	
		
		try {
			ExecutableContainer<?,?,ExecuteState,Dataset> ec = (ExecutableContainer)oc;
			IntermediatePublishableContainer<?,?,ExecuteState, PublishState<ExecuteState>,Dataset> pc = (IntermediatePublishableContainer)oc;
			
			ProcessStateContainer psv = pc.getCurrentPublishState();
			if (psv == null) {
				return new ResponseEntity<>(HttpStatus.NO_CONTENT);			
			}
			
			MappingPublishState ps = (MappingPublishState)psv.getProcessState();
			MappingExecuteState pes = ps.getExecute();
			
			if (!pes.getDatabaseConfigurationId().equals(fileSystemConfiguration.getId())) {
				return new ResponseEntity<>(HttpStatus.NO_CONTENT);
			}
			
			if (pes.getExecuteState() != MappingState.EXECUTED) {
				return new ResponseEntity<>(HttpStatus.NO_CONTENT);
			}
			
			File file = folderService.getExecutionFile(ec, pes, FileType.zip);

			// for compatibility
			if (file == null) {
				file = serviceUtils.zipExecution(ec, pes, pes.getExecuteShards() == 0 ? 1 : pes.getExecuteShards());
			}			
			
			return downloadFile(file.getAbsolutePath());

		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
	}
	
	public ResponseEntity<StreamingResponseBody> downloadFile(String filename) throws IOException {
		return downloadFile(filename, null);
	}
		
	public ResponseEntity<StreamingResponseBody> downloadFile(String filename, String returnFilename) throws IOException {
		File ffile = new File(filename);
   	
	    HttpHeaders headers = new HttpHeaders();
	    headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
	    headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + (returnFilename == null ? ffile.getName() : returnFilename));
	    headers.set(HttpHeaders.CONTENT_LENGTH, String.valueOf(ffile.length()));
	    
	    StreamingResponseBody stream = outputStream -> {
	    	int bytesRead;
	        byte[] buffer = new byte[2048];
	        
	        try (InputStream inputStream = new FileInputStream(ffile)) {
		        while ((bytesRead = inputStream.read(buffer)) != -1) {
		            outputStream.write(buffer, 0, bytesRead);
		        }
	        }		        
	    };
	    
	    return ResponseEntity.ok()
	            .headers(headers)
	            .contentLength(ffile.length())
	            .contentType(MediaType.APPLICATION_OCTET_STREAM)
	            .body(stream);
	}
	
	public ResponseEntity<StreamingResponseBody> downloadFile(byte[] data, String fileName) throws IOException {
   	
	    HttpHeaders headers = new HttpHeaders();
	    headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
	    headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + fileName);
	    headers.set(HttpHeaders.CONTENT_LENGTH, String.valueOf(data.length));
	    
	    StreamingResponseBody stream = outputStream -> {
	    	int bytesRead;
	        byte[] buffer = new byte[2048];
	        
	        try (InputStream inputStream = new ByteArrayInputStream(data)) {
		        while ((bytesRead = inputStream.read(buffer)) != -1) {
		            outputStream.write(buffer, 0, bytesRead);
		        }
	        }		        
	    };
	    
	    return ResponseEntity.ok()
	            .headers(headers)
	            .contentLength(data.length)
	            .contentType(MediaType.APPLICATION_OCTET_STREAM)
	            .body(stream);
	}

	public <D extends SpecificationDocument, F extends Response, I extends EnclosingDocument> 
		ListResult<F> getAllByUser(UserPrincipal currentUser, UserPrincipal owner, List<ObjectId> datasetId, EnclosingService<D,F,I> service, ContainerService<I,?> enclosingService) {
		return 	getAllByUser(currentUser, owner, datasetId, service, enclosingService, null, null);
	}

	public <D extends SpecificationDocument, F extends Response, I extends EnclosingDocument> 
		ListResult<F> getAllByUser(UserPrincipal currentUser, UserPrincipal owner,  List<ObjectId> datasetId, EnclosingService<D,F,I> service, ContainerService<I,?> enclosingService, Pageable page) {
		return 	getAllByUser(currentUser, owner, datasetId, service, enclosingService, null, page);
	}
	
	public <D extends SpecificationDocument, F extends Response, I extends EnclosingDocument, M extends ResponseModifier> 
		ListResult<F> getAllByUser(UserPrincipal currentUser, UserPrincipal owner, List<ObjectId> datasetId, EnclosingService<D,F,I> service, ContainerService<I,?> enclosingService, M mr, Pageable page) {
	
		if (datasetId != null) {
			
//			ObjectContainer<I,?> ec = enclosingService.getContainer(currentUser, new SimpleObjectIdentifier(datasetId));
//			
//			List<F> res = new ArrayList<>();
//			Pagination pg = null;
//	
//			if (ec != null) {
//				I enclosingObject = ec.getObject();
//				
//				ListPage<D> list = service.getAllByUser((List<I>)Arrays.asList(new EnclosingDocument[] {enclosingObject}), owner != null ? new ObjectId(owner.getId()) : null, page);
//				
//				for (D doc : list.getList()) {
//					ObjectContainer<D,F> oc = service.getContainer(currentUser, doc, enclosingObject);
//					
//					res.add(toResponse(oc, mr));
//				}
//				
//				pg = list.getPagination();
//			}

			List<F> res = new ArrayList<>();
			Pagination pg = null;

			List<I> enclosingObject = new ArrayList<>();
			for (ObjectId inId : datasetId) {
				ObjectContainer<I,?> ec = enclosingService.getContainer(currentUser, new SimpleObjectIdentifier(inId));
				if (ec != null) {
					enclosingObject.add(ec.getObject());
				}
			}
			
	
			ListPage<D> list = service.getAllByUser(enclosingObject, owner != null ? new ObjectId(owner.getId()) : null, page);
				
			for (D doc : list.getList()) {
				ObjectContainer<D,F> oc = service.getContainer(currentUser, doc);
				
				res.add(toResponse(oc, mr));
			}
			
			pg = list.getPagination();
			
			return new ListResult<F>(res, pg);
	
		} else {
			return getAllByUser(currentUser, owner, service);
		}
	}	

	public <D extends SpecificationDocument, F extends Response, I extends EnclosingDocument, L extends LookupProperties> 
		ListResult<F> getAllByUser(UserPrincipal currentUser, UserPrincipal owner, List<ObjectId> insideId, L lp, EnclosingLookupService<D,F,I,L> service, ContainerService<I,?> enclosingService) {
		return 	getAllByUser(currentUser, owner, insideId, lp, service, enclosingService, null, null);
	}
	
	public <D extends SpecificationDocument, F extends Response, I extends EnclosingDocument, L extends LookupProperties> 
		ListResult<F> getAllByUser(UserPrincipal currentUser,UserPrincipal owner,  List<ObjectId> insideId, L lp, EnclosingLookupService<D,F,I,L> service, ContainerService<I,?> enclosingService, Pageable page) {
		return 	getAllByUser(currentUser, owner, insideId, lp, service, enclosingService, null, page);
	}

	public <D extends SpecificationDocument, F extends Response, I extends EnclosingDocument, L extends LookupProperties, M extends ResponseModifier> 
		ListResult<F> getAllByUser(UserPrincipal currentUser, UserPrincipal owner, List<ObjectId> insideId, L lp, EnclosingLookupService<D,F,I,L> service, ContainerService<I,?> enclosingService, M mr, Pageable page) {

//		if (insideId == null) {
//			return getAllByUser(currentUser, owner, lp, (LookupService<D,F,L>)service, mr, page);
//		} else {
//			List<F> res = new ArrayList<>();
//			Pagination pg = null;
//
//			ObjectContainer<I,?> ec = enclosingService.getContainer(currentUser, new SimpleObjectIdentifier(insideId));
//	
//			if (ec != null) {
//				I enclosingObject = ec.getObject();
//				
//				ListPage<D> list = service.getAllByUser(enclosingObject, owner != null ? new ObjectId(owner.getId()) : null, lp, page);
//				for (D doc : list.getList()) {
//					ObjectContainer<D,F> oc = service.getContainer(currentUser, doc, enclosingObject);
//					
//					res.add(toResponse(oc, mr));
//				}
//				
//				pg = list.getPagination();
//			}
//			
//			return new ListResult<F>(res, pg);
//		}		
		
		if (insideId == null) {
			return getAllByUser(currentUser, owner, lp, (LookupService<D,F,L>)service, mr, page);
		} else {
			
			List<I> enclosingObject = new ArrayList<>();
			for (ObjectId inId : insideId) {
				ObjectContainer<I,?> ec = enclosingService.getContainer(currentUser, new SimpleObjectIdentifier(inId));
				if (ec != null) {
					enclosingObject.add(ec.getObject());
				}
			}

			List<F> res = new ArrayList<>();
			Pagination pg = null;

			ListPage<D> list = service.getAllByUser(enclosingObject, owner != null ? new ObjectId(owner.getId()) : null, lp, page);
			for (D doc : list.getList()) {
				ObjectContainer<D,F> oc = service.getContainer(currentUser, doc);
					
				res.add(toResponse(oc, mr));
			}
			
			pg = list.getPagination();
			
			return new ListResult<F>(res, pg);
		}	
		
	}	

	public <D extends SpecificationDocument, F extends Response> 
		ListResult<F> getAllByUser(UserPrincipal currentUser, UserPrincipal owner, ContainerService<D,F> service) {
		return getAllByUser(currentUser, owner, service, null, null);
	}
	
	public <D extends SpecificationDocument, F extends Response> 
		ListResult<F> getAllByUser(UserPrincipal currentUser, UserPrincipal owner, ContainerService<D,F> service, Pageable page) {
		return getAllByUser(currentUser, owner, service, null, page);
	}
	
	public <D extends SpecificationDocument, F extends Response, M extends ResponseModifier> 
		ListResult<F> getAllByUser(UserPrincipal currentUser, UserPrincipal owner, ContainerService<D,F> service, M mr, Pageable page) {
	
		List<F> res = new ArrayList<>();
	
		ListPage<D> list = service.getAllByUser(owner != null ? new ObjectId(owner.getId()) : null, page);
		
		for (D doc : list.getList()) {
			ObjectContainer<D,F> oc = service.getContainer(currentUser, doc);
				
			res.add(toResponse(oc, mr));
		}
		
		return new ListResult<F>(res, list.getPagination());
	}

	public <D extends SpecificationDocument, F extends Response, L extends LookupProperties> 
		ListResult<F> getAllByUser(UserPrincipal currentUser, UserPrincipal owner, L lp, LookupService<D,F,L> service) {
		return getAllByUser(currentUser, owner, lp, service, null, null);
	}
	
	public <D extends SpecificationDocument, F extends Response, L extends LookupProperties> 
		ListResult<F> getAllByUser(UserPrincipal currentUser, UserPrincipal owner, L lp, LookupService<D,F,L> service, Pageable page) {
		return getAllByUser(currentUser, owner, lp, service, null, page);
	}
	
	public <D extends SpecificationDocument, F extends Response, L extends LookupProperties, M extends ResponseModifier> 
		ListResult<F> getAllByUser(UserPrincipal currentUser, UserPrincipal owner, L lp, LookupService<D,F,L> service, M mr, Pageable page) {
	
		List<F> res = new ArrayList<>();
	
		ListPage<D> list = service.getAllByUser(owner != null ? new ObjectId(owner.getId()) : null, lp, page);
		
		for (D doc : list.getList()) {
			ObjectContainer<D,F> oc = service.getContainer(currentUser, doc);
				
			res.add(toResponse(oc, mr));
		}
		
		return new ListResult<F>(res, list.getPagination());
	}

	public <D extends SpecificationDocument, F extends Response, L extends LookupProperties> 
		ListResult<F> getAll(L lp, LookupService<D,F,L> service) {
		return getAll(lp, service, null, null);
	}
	
	public <D extends SpecificationDocument, F extends Response, L extends LookupProperties> 
		ListResult<F> getAll(L lp, LookupService<D,F,L> service, Pageable page) {
		return getAll(lp, service, null, page);
	}
	
	public <D extends SpecificationDocument, F extends Response, L extends LookupProperties, M extends ResponseModifier> 
		ListResult<F> getAll(L lp, LookupService<D,F,L> service, M mr, Pageable page) {
	
		List<F> res = new ArrayList<>();
	
		ListPage<D> list = service.getAll(lp, page);
		
		for (D doc : list.getList()) {
			ObjectContainer<D,F> oc = service.getContainer(null, doc);
				
			res.add(toResponse(oc, mr));
		}
		
		return new ListResult<F>(res, list.getPagination());
	}
	
	public <D extends SpecificationDocument, F extends Response, I extends EnclosingDocument, L extends LookupProperties> 
		ListResult<F> getAll(ObjectId insideId, L lp, EnclosingLookupService<D,F,I,L> service, ContainerService<I,?> enclosingService) {
		return getAll(insideId, lp, service, enclosingService, null, null);
	}
	
	public <D extends SpecificationDocument, F extends Response, I extends EnclosingDocument, L extends LookupProperties> 
		ListResult<F> getAll(ObjectId insideId, L lp, EnclosingLookupService<D,F,I,L> service, ContainerService<I,?> enclosingService, Pageable page) {
		return getAll(insideId, lp, service, enclosingService, null, page);
	}
	
	public <D extends SpecificationDocument, F extends Response, I extends EnclosingDocument, L extends LookupProperties, M extends ResponseModifier> 
		ListResult<F> getAll(ObjectId insideId, L lp, EnclosingLookupService<D,F,I,L> service, ContainerService<I,?> enclosingService, M mr, Pageable page) {
		
		if (insideId == null) {
			return getAll(lp, (LookupService<D,F,L>)service, mr, page);
		} else {
			ObjectContainer<I,?> ec = enclosingService.getContainer(null, new SimpleObjectIdentifier(insideId));

			List<F> res = new ArrayList<>();
			Pagination pg = null;
	
			if (ec != null) {
				I enclosingObject = ec.getObject();
				
				ListPage<D> list = service.getAll((List<I>)Arrays.asList(new EnclosingDocument[] {enclosingObject}), lp, page);
				for (D doc : list.getList()) {
					ObjectContainer<D,F> oc = service.getContainer(null, doc, enclosingObject);
					
					res.add(toResponse(oc, mr));
				}
				
				pg = list.getPagination();
			}
			
			return new ListResult(res, pg);
		}
	}	

	private <D extends SpecificationDocument, F extends Response, M extends ResponseModifier> 
		F toResponse(ObjectContainer<D,F> oc, M rm) {
		
		if (rm == null) {
			return oc.asResponse();
		} else {
			return ((MultipleResponseContainer<D,F,M>)oc).asResponse(rm);
		}
		
	}
	
	private <D extends SpecificationDocument, F extends Response> Class<? extends UpdateRequest> 
		getUpdateRequestClass(ContainerService<D,F> containerService) {

		for (Type type : ClassUtils.getUserClass(containerService).getGenericInterfaces()) {
			if (type instanceof ParameterizedType) {
				ParameterizedType paramType = (ParameterizedType)type;

				if (paramType.getRawType().equals(EnclosedCreatableService.class) || paramType.getRawType().equals(EnclosedCreatableLookupService.class)) {
					for (Type t : paramType.getActualTypeArguments()) {
						Class cz = null;
						try {
							cz = Class.forName(t.getTypeName());
						} catch (ClassNotFoundException ex) {
							continue;
						}
						for (Class inter : cz.getInterfaces()) {
							if (inter.equals(MultipartFileUpdateRequest.class)) { /// !!! checks only direct superclasses
								return (Class<? extends UpdateRequest>)t;
							}
						}
					}
				}
			}			
		}
		
		return null;
	}

	private <D extends SpecificationDocument, F extends Response> 
		MultipartFileUpdateRequest buildMultipartFileUpdateRequest(String json, MultipartFile file, ContainerService<D,F> containerService) throws Exception {
		
		ObjectMapper objectMapper = new ObjectMapper();
	
		Class<? extends UpdateRequest> cz = getUpdateRequestClass(containerService);
		
		MultipartFileUpdateRequest ur = (MultipartFileUpdateRequest)objectMapper.readValue(json, cz);
		ur.setFile(file);

		return ur;
	}
	
	public Pageable pageable(Integer page, int size) {
		if (page == null) {
			return null;
		} else {
			return PageRequest.of(page - 1, size);
		}
	}
	
	public String generateNameFromUrl(String url){

	    // Replace useless characters with UNDERSCORE
	    String uniqueName = url.replace("://", "_").replace(".", "_").replace("/", "_");
	    // Replace last UNDERSCORE with a DOT
	    uniqueName = uniqueName.substring(0,uniqueName.lastIndexOf('_'))
	            +"."+uniqueName.substring(uniqueName.lastIndexOf('_')+1,uniqueName.length());
	    return uniqueName;
	}
	
	public Map<String, Object> jsonLDFrame(Model model, Map frame) {
        JsonLdOptions options = new JsonLdOptions();
        options.setCompactArrays(true);
        options.setUseNativeTypes(true); 	      
        options.setOmitGraph(false);
        options.setPruneBlankNodeIdentifiers(true);
        options.setEmbed(true);
        
        final RDFDataset jsonldDataset = (new JenaRDF2JSONLD()).parse(DatasetFactory.wrap(model).asDatasetGraph());
        Object obj = (new JsonLdApi(options)).fromRDF(jsonldDataset, true);
        
        Map<String, Object> jn = JsonLdProcessor.frame(obj, frame, options);
        
        return jn;
	}
}
