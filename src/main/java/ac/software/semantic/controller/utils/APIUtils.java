package ac.software.semantic.controller.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ExecutableContainer;
import ac.software.semantic.service.ExecutingService;
import ac.software.semantic.service.FolderService;
import ac.software.semantic.service.IntermediatePublishableContainer;
import ac.software.semantic.service.ObjectContainer;
import ac.software.semantic.service.ObjectIdentifier;
import ac.software.semantic.service.PublishableContainer;
import ac.software.semantic.service.PublishingService;
import ac.software.semantic.service.ServiceUtils;
import ac.software.semantic.service.TaskConflictException;
import ac.software.semantic.service.TaskService;
import ac.software.semantic.service.TaskSpecification;

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
	private TaskService taskService;
	
	
	public ResponseEntity<APIResponse> execute(UserPrincipal currentUser, ObjectIdentifier objId, ExecutingService containerService)  {

		Class<? extends ObjectContainer> containerClass = containerService.getContainerClass();
		
		ObjectContainer oc = null;
		ExecutableContainer ec = null;
		
		try {
			oc = containerService.getContainer(currentUser, objId);
			ec = (ExecutableContainer)oc;
			
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
		
		try {
			TaskDescription tdescr = TaskSpecification.getTaskSpecification(ec.getExecuteTask()).createTask(oc);
			
	    	if (tdescr != null) {
	    		taskService.call(tdescr);
	    		return APIResponse.acceptedToExecute(ec);
	    	} else {
	    		return new ResponseEntity<>(APIResponse.FailureResponse("Server error."), HttpStatus.INTERNAL_SERVER_ERROR);
	    	}
		
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}	
	}	
	
	public ResponseEntity<APIResponse> publish(UserPrincipal currentUser, ObjectIdentifier objId, PublishingService containerService)  {
			
		Class<? extends ObjectContainer> containerClass = containerService.getContainerClass();
		
		synchronized (containerService.synchronizedString(objId.toHexString())) {
			ObjectContainer oc = null;
			PublishableContainer pc = null;
			
			try {
		    	oc = containerService.getContainer(currentUser, objId);
		    	pc = (PublishableContainer)oc;
		    	
		    	if (oc == null) {
		    		return APIResponse.notFound(containerClass);
		    	} else if (pc.isPublished()) {
		    		throw TaskConflictException.alreadyPublished(pc);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.badRequest();
			}
			
			try {
				TaskDescription tdescr = TaskSpecification.getTaskSpecification(pc.getPublishTask()).createTask(oc);
				
		    	if (tdescr != null) {
		    		taskService.call(tdescr);
		    		return APIResponse.acceptedToPublish(pc);
		    	} else {
		    		return new ResponseEntity<>(APIResponse.FailureResponse("Server error."), HttpStatus.INTERNAL_SERVER_ERROR);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);				
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex);
			}
		}
	}    
	
	public ResponseEntity<APIResponse> unpublish(UserPrincipal currentUser, ObjectIdentifier objId, PublishingService containerService)  {
		
		Class<? extends ObjectContainer> containerClass = containerService.getContainerClass();
		
		synchronized (containerService.synchronizedString(objId.toHexString())) {
			ObjectContainer oc = null;
			PublishableContainer pc = null;
			
			try {
		    	oc = containerService.getContainer(currentUser, objId);
		    	pc = (PublishableContainer)oc;
		    	
		    	if (oc == null) {
		    		return APIResponse.notFound(containerClass);
		    	} else if (!pc.isPublished()) {
		    		throw TaskConflictException.notPublished(pc);
		    	}
			} catch (TaskConflictException ex) {
				return APIResponse.conflict(ex);
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.badRequest();
			}
			
			try {
		    	TaskDescription tdescr = TaskSpecification.getTaskSpecification(pc.getUnpublishTask()).createTask(oc);
				
		    	if (tdescr != null) {
		    		taskService.call(tdescr);
		    		return APIResponse.acceptedToUnpublish(pc);
		    	} else {
		    		return new ResponseEntity<>(APIResponse.FailureResponse("Server error."), HttpStatus.INTERNAL_SERVER_ERROR);
		    	}
			
			} catch (Exception ex) {
				ex.printStackTrace();
				return APIResponse.serverError(ex);
			}
		}
	}   	
	
 	public ResponseEntity<APIResponse> clearExecution(UserPrincipal currentUser, ObjectIdentifier objId, ExecutingService containerService)  {
 		
 		Class<? extends ObjectContainer> containerClass = containerService.getContainerClass();
 		
 		ObjectContainer oc = null;
 		ExecutableContainer ec;
		try {
			oc = containerService.getContainer(currentUser, objId);
			ec = (ExecutableContainer)oc;
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
		
		try {
			synchronized (oc.synchronizationString(ec.getClearLastExecutionTask())) {
				boolean ok = ((ExecutableContainer)oc).clearExecution();
				if (ok) {
					return APIResponse.executionDeleted(oc);
				} else { // incorrect... should work out actual errors
					return APIResponse.executionDeleteError(oc); 
				}
			}			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
 	}	

	public ResponseEntity<?> previewLastExecution(UserPrincipal currentUser, ObjectIdentifier objId, ExecutingService containerService)  {
		
		Class<? extends ObjectContainer> containerClass = containerService.getContainerClass();
		
		ExecutableContainer ec = null;
		try {
			ec = (ExecutableContainer)containerService.getContainer(currentUser, objId);
	    	if (ec == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}
		
		try {
			ExecuteState es = ec.getExecuteState();

			if (es.getExecuteState() != MappingState.EXECUTED) {
				return new ResponseEntity<>(HttpStatus.NO_CONTENT);
			}

			File file = folderService.getExecutionTrigFile(ec, es, 0);
			
			if (file != null) {
				String res = FileUtils.readFileBeginning(Paths.get(file.getAbsolutePath()));
				return ResponseEntity.ok(res);
			
			} else {
				return ResponseEntity.ok("Execution is empty.");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}	
	
	// fix handle error responses
	public ResponseEntity<?> previewPublishedExecution(UserPrincipal currentUser, ObjectIdentifier objId, ExecutingService containerService)  {

		Class<? extends ObjectContainer> containerClass = containerService.getContainerClass();
		
		ObjectContainer oc = null;
		try {
			oc = containerService.getContainer(currentUser, objId);
	    	if (oc == null) {
	    		return APIResponse.notFound(containerClass);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.badRequest();
		}		
		
		try {
			ExecutableContainer ec = (ExecutableContainer)oc;
			IntermediatePublishableContainer pc = (IntermediatePublishableContainer)oc;
			
			ProcessStateContainer psv = pc.getCurrentPublishState();
			if (psv == null) {
				return new ResponseEntity<>(HttpStatus.NO_CONTENT);			
			}
			
			MappingPublishState ps = (MappingPublishState)psv.getProcessState();
			ExecuteState pes = ps.getExecute();
			
			if (!pes.getDatabaseConfigurationId().equals(fileSystemConfiguration.getId())) {
				return new ResponseEntity<>(HttpStatus.NO_CONTENT);
			}
			
			if (pes.getExecuteState() != MappingState.EXECUTED) {
				return new ResponseEntity<>(HttpStatus.NO_CONTENT);
			}

			File file = folderService.getExecutionTrigFile(ec, pes, 0);
			
			if (file != null) {
				String res = FileUtils.readFileBeginning(Paths.get(file.getAbsolutePath()));
				return ResponseEntity.ok(res);
			} else {
				return ResponseEntity.ok("Execution is empty.");
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex);
		}
	}
	
	public ResponseEntity<StreamingResponseBody> downloadLastExecution(UserPrincipal currentUser, ObjectIdentifier objId, ExecutingService containerService)  {
	
//		Class<? extends ObjectContainer> containerClass = containerService.getContainerClass();
		
		ExecutableContainer ec = null;
		try {
			ec = (ExecutableContainer)containerService.getContainer(currentUser, objId);
	    	if (ec == null) {
	    		return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    	}
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}	
		
		try {
			MappingExecuteState es = ec.getExecuteState();  // TODO: Should check that it is executed
					
			File file = folderService.getExecutionZipFile(ec, es);
			
			if (file == null) {
				file = serviceUtils.zipExecution(ec, es, es.getExecuteShards() == 0 ? 1 : es.getExecuteShards());
			}	

			return downloadFile(file.getAbsolutePath());

		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
	}    
	
	public ResponseEntity<StreamingResponseBody> downloadPublishedExecution(UserPrincipal currentUser, ObjectIdentifier objId, ExecutingService containerService)  {
	
//		Class<? extends ObjectContainer> containerClass = containerService.getContainerClass();
		
		ObjectContainer oc = null;
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
			ExecutableContainer ec = (ExecutableContainer)oc;
			IntermediatePublishableContainer pc = (IntermediatePublishableContainer)oc;
			
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
			
			File file = folderService.getExecutionZipFile(ec, pes);

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
		File ffile = new File(filename);
   	
	    HttpHeaders headers = new HttpHeaders();
	    headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
	    headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + ffile.getName());
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
}
