package ac.software.semantic.payload;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.ExecutableContainer;
import ac.software.semantic.service.FilterAnnotationValidationService.FilterAnnotationValidationContainer;
import ac.software.semantic.service.MappingsService.MappingContainer;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import ac.software.semantic.service.PublishableContainer;
import ac.software.semantic.service.ObjectContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class APIResponse {
    private Boolean success;
    private String message;
    private Object result;

    public static APIResponse SuccessResponse(String message) {
    	return new APIResponse(true, message);
    }
    
    public static APIResponse SuccessResponse(String message, Object result) {
    	return new APIResponse(true, message, result);
    }
    
    public static APIResponse SuccessResponse() {
    	return new APIResponse(true, "");
    }

    public static APIResponse FailureResponse(String message) {
    	return new APIResponse(false, message);
    }

    public static APIResponse FailureResponse() {
    	return new APIResponse(false, "");
    }

    public APIResponse(Boolean success, String message) {
        this(success, message, null);
    }

    public APIResponse(Boolean success, String message, Object result) {
        this.success = success;
        this.message = message;
        this.result = result;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}
	
	public static ResponseEntity<APIResponse> notFound(Class<? extends ObjectContainer> clazz) {
		return new ResponseEntity<>(FailureResponse("The " + className(clazz) + " was not found."), HttpStatus.NOT_FOUND);
	}
	
	public static ResponseEntity<APIResponse> badRequest() {
		return new ResponseEntity<>(FailureResponse("Bad request."), HttpStatus.BAD_REQUEST);
	}

	public static ResponseEntity<APIResponse> methodNotAllowed() {
		return new ResponseEntity<>(FailureResponse("Method not allowed."), HttpStatus.METHOD_NOT_ALLOWED);
	}

	public static ResponseEntity<APIResponse> created(ObjectContainer oc) {
		return new ResponseEntity<>(SuccessResponse("The " +  className(oc.getClass()) + " has been created.", oc.asResponse()), HttpStatus.OK);
	}

	public static ResponseEntity<APIResponse> updated(ObjectContainer oc) {
		return new ResponseEntity<>(SuccessResponse("The " +  className(oc.getClass()) + " has been updated.", oc.asResponse()), HttpStatus.OK);
	}

	public static ResponseEntity<APIResponse> deleted(ObjectContainer oc) {
		return new ResponseEntity<>(SuccessResponse("The " +  className(oc.getClass()) + " has been deleted."), HttpStatus.OK);
	}
	
	public static ResponseEntity<APIResponse> executionDeleted(ObjectContainer oc) {
		return new ResponseEntity<>(SuccessResponse("The " +  className(oc.getClass()) + " execution has been deleted.", oc.asResponse()), HttpStatus.OK);
	}

	public static ResponseEntity<APIResponse> executionDeleteError(ObjectContainer oc) {
		return new ResponseEntity<>(FailureResponse("Failed to delete " +  className(oc.getClass()) + " execution."), HttpStatus.INTERNAL_SERVER_ERROR);
	}

	public static ResponseEntity<APIResponse> acceptedToExecute(ExecutableContainer oc) {
		return new ResponseEntity<>(SuccessResponse("The " +  className(oc.getClass()) + " has been queued for execution."), HttpStatus.ACCEPTED);
	}

	public static ResponseEntity<APIResponse> acceptedToPublish(PublishableContainer oc) {
		return new ResponseEntity<>(SuccessResponse("The " +  className(oc.getClass()) + " has been queued for publication."), HttpStatus.ACCEPTED);
	}

	public static ResponseEntity<APIResponse> acceptedToUnpublish(PublishableContainer oc) {
		return new ResponseEntity<>(SuccessResponse("The " +  className(oc.getClass()) + " has been queued for unpublication."), HttpStatus.ACCEPTED);
	}

	public static ResponseEntity<APIResponse> acceptedToIndex(ObjectContainer oc) {
		return new ResponseEntity<>(SuccessResponse("The " +  className(oc.getClass()) + " has been queued for indexing."), HttpStatus.ACCEPTED);
	}

	public static ResponseEntity<APIResponse> acceptedToUnindex(ObjectContainer oc) {
		return new ResponseEntity<>(SuccessResponse("The " +  className(oc.getClass()) + " has been queued for unindexing."), HttpStatus.ACCEPTED);
	}

	public static ResponseEntity<APIResponse> conflict(Throwable ex) {
		return new ResponseEntity<>(FailureResponse(ex.getMessage()), HttpStatus.CONFLICT);
	}

	public static ResponseEntity<APIResponse> serverError(Throwable ex) {
		return new ResponseEntity<>(FailureResponse(ex.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
	}
	
	public static ResponseEntity<APIResponse> retrieved(ObjectContainer oc) {
		return new ResponseEntity<>(SuccessResponse(null, oc.asResponse()), HttpStatus.OK);
	}


	public static String className(Class<?> clazz) {
		if (clazz == AnnotatorContainer.class) {
			return "annnotator";
		} else if (clazz == EmbedderContainer.class) {
			return "embedder";
		} else if (clazz == MappingContainer.class) {
			return "mapping";
		} else if (clazz == DatasetContainer.class) {
			return "dataset";
		} else if (clazz == PagedAnnotationValidationContainer.class) {
			return "paged annotation validation";
		} else if (clazz == FilterAnnotationValidationContainer.class) {
			return "filter annotation validation";
		} else {
			return "";
		}
	}
}
