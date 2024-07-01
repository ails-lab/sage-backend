package ac.software.semantic.payload.response;

import java.util.ArrayList;
import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.Pagination;
import ac.software.semantic.model.base.ContentDocument;
import ac.software.semantic.model.base.MemberDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.ListResult;
import ac.software.semantic.payload.response.modifier.ResponseModifier;
import ac.software.semantic.security.SelectRoleResponse;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.CampaignService.CampaignContainer;
import ac.software.semantic.service.ClustererService.ClustererContainer;
import ac.software.semantic.service.ComparatorService.ComparatorContainer;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.FileService.FileContainer;
import ac.software.semantic.service.FilterAnnotationValidationService.FilterAnnotationValidationContainer;
import ac.software.semantic.service.IndexService.IndexContainer;
import ac.software.semantic.service.MappingService.MappingContainer;
import ac.software.semantic.service.PagedAnnotationValidationService.PagedAnnotationValidationContainer;
import ac.software.semantic.service.ProjectService.ProjectContainer;
import ac.software.semantic.service.UserService.UserContainer;
import ac.software.semantic.service.UserTaskService.UserTaskContainer;
import ac.software.semantic.service.container.CreatableContainer;
import ac.software.semantic.service.container.ExecutableContainer;
import ac.software.semantic.service.container.InverseMemberContainer;
import ac.software.semantic.service.container.MemberContainer;
import ac.software.semantic.service.container.MultipleResponseContainer;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.PreparableContainer;
import ac.software.semantic.service.container.PublishableContainer;
import ac.software.semantic.service.container.ValidatableContainer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class APIResponse {
    private Boolean success;
    private String message;
    private Object data;
    private Object metadata;
    private Pagination pagination;
    
    @JsonIgnore
    private HttpStatus httpStatus;

    public static APIResponse SuccessResponse(String message, HttpStatus httpStatus) {
    	return new APIResponse(true, message, httpStatus);
    }

    public static APIResponse SuccessResponse(String message, Object data, HttpStatus httpStatus) {
    	return new APIResponse(true, message, data, httpStatus);
    }

    public static APIResponse SuccessResponse(String message, List<?> data, Object metadata, Pagination pg, HttpStatus httpStatus) {
    	return new APIResponse(true, message, data, metadata, pg, httpStatus);
    }

    public static APIResponse SuccessResponse(HttpStatus httpStatus) {
    	return new APIResponse(true, "", httpStatus);
    }

    public static APIResponse FailureResponse(String message, HttpStatus httpStatus) {
    	return new APIResponse(false, message, httpStatus );
    }

    public static APIResponse FailureResponse(HttpStatus httpStatus) {
    	return new APIResponse(false, "", httpStatus);
    }

    public APIResponse(Boolean success, String message, HttpStatus httpStatus) {
        this(success, message, null, httpStatus);
    }

    public APIResponse(Boolean success, String message, Object data, HttpStatus httpStatus) {
        this.success = success;
        this.message = message;
       	this.data = data;
       	this.httpStatus = httpStatus;
    }
    
    public APIResponse(Boolean success, String message, List<?> data, Pagination pg, HttpStatus httpStatus) {
        this.success = success;
        this.message = message;
       	this.data = data;
       	this.pagination = pg;
       	this.httpStatus = httpStatus;
    }
    
    public APIResponse(Boolean success, String message, List<?> data, Object metadata, Pagination pg, HttpStatus httpStatus) {
        this.success = success;
        this.message = message;
       	this.data = data;
       	this.metadata = metadata;
       	this.pagination = pg;
       	this.httpStatus = httpStatus;
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

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public ResponseEntity<APIResponse> toResponseEntity() {
		return new ResponseEntity<>(this, this.getHttpStatus());
	}
	
	public static APIResponse noContent() {
		return SuccessResponse("No content.", HttpStatus.NO_CONTENT);
	}

	public static APIResponse notFound() {
		return FailureResponse("Not found.", HttpStatus.NOT_FOUND);
	}

	public static APIResponse notFound(Class<? extends ObjectContainer<?,?>> clazz) {
		return FailureResponse("The " + className(clazz) + " was not found.", HttpStatus.NOT_FOUND);
	}

	public static APIResponse badRequest() {
		return FailureResponse("Bad request.", HttpStatus.BAD_REQUEST);
	}

	public static APIResponse invalidCronExpression() {
		return FailureResponse("Invalid chron expression.", HttpStatus.BAD_REQUEST);
	}

	public static APIResponse notLoggedIn() {
		return FailureResponse("Not logged in.", HttpStatus.UNAUTHORIZED);
	}

	public static APIResponse ok() {
		return SuccessResponse("Ok.", HttpStatus.OK);
	}

	public static APIResponse serverError() {
		return FailureResponse("Internal server error.", HttpStatus.INTERNAL_SERVER_ERROR);
	}
		
	public static APIResponse methodNotAllowed() {
		return FailureResponse("Method not allowed.", HttpStatus.METHOD_NOT_ALLOWED);
	}

	public static APIResponse couldNotStopExecution(ExecutableContainer<?,?,?,?> oc) {
		return FailureResponse("The " +  className(oc.getClass()) + " could not be stoped.", HttpStatus.CONFLICT);
	}

	public static APIResponse couldNotStopCreating(CreatableContainer<?,?,?,?> oc) {
		return FailureResponse("Creating of " +  className(oc.getClass()) + " could not be stoped.", HttpStatus.CONFLICT);
	}

	public static APIResponse created(ObjectContainer<?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " has been created.", oc.asResponse(), HttpStatus.OK);
	}

	public static APIResponse updated(ObjectContainer<?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " has been updated.", oc.asResponse(), HttpStatus.OK);
	}

	public static APIResponse ready(PreparableContainer rc) {
		return SuccessResponse("The " +  className(rc.getClass()) + " is ready.", HttpStatus.OK);
	}

	public static APIResponse notExecuting(ExecutableContainer<?,?,?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " is not being executed.", ((ObjectContainer<?,?>)oc).asResponse(), HttpStatus.OK);
	}

	public static APIResponse notCreating(CreatableContainer<?,?,?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " is not being created.", ((ObjectContainer<?,?>)oc).asResponse(), HttpStatus.OK);
	}

	public static APIResponse deleted(ObjectContainer<?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " has been deleted.", HttpStatus.OK);
	}

	public static APIResponse deleted(ObjectContainer<?,?> oc, List<Response> res) {
		return SuccessResponse("The " +  className(oc.getClass()) + " has been deleted.", res, HttpStatus.OK);
	}
	
	public static APIResponse executionDeleted(ObjectContainer<?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " execution has been deleted.", oc.asResponse(), HttpStatus.OK);
	}

	public static APIResponse executionDeleteError(ObjectContainer<?,?> oc) {
		return FailureResponse("Failed to delete " +  className(oc.getClass()) + " execution.", HttpStatus.INTERNAL_SERVER_ERROR);
	}

	public static APIResponse acceptedToExecute(ExecutableContainer<?,?,?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " has been queued for execution.", HttpStatus.ACCEPTED);
	}

	public static APIResponse acceptedToValidate(ValidatableContainer<?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " has been queued for validation.", HttpStatus.ACCEPTED);
	}

	public static APIResponse acceptedToStopExecution(ExecutableContainer<?,?,?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " is being stopped.", HttpStatus.ACCEPTED);
	}

	public static APIResponse acceptedToStopCreating(CreatableContainer<?,?,?,?> oc) {
		return SuccessResponse("Creating of " +  className(oc.getClass()) + " is being stopped.", HttpStatus.ACCEPTED);
	}

	public static APIResponse acceptedToPublish(PublishableContainer<?,?,?,?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " has been queued for publication.", HttpStatus.ACCEPTED);
	}

	public static APIResponse acceptedToUnpublish(PublishableContainer<?,?,?,?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " has been queued for unpublication.", HttpStatus.ACCEPTED);
	}

	public static APIResponse acceptedToRepublish(PublishableContainer<?,?,?,?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " has been queued for republication.", HttpStatus.ACCEPTED);
	}

	public static APIResponse acceptedToCreate(CreatableContainer<?,?,?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " has been queued for creating.", HttpStatus.ACCEPTED);
	}

	public static APIResponse acceptedToDestroy(CreatableContainer<?,?,?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " has been queued for destroying.", HttpStatus.ACCEPTED);
	}
	
	public static APIResponse acceptedToRecreate(CreatableContainer<?,?,?,?> oc) {
		return SuccessResponse("The " +  className(oc.getClass()) + " has been queued for recreating.", HttpStatus.ACCEPTED);
	}


	public static APIResponse conflict(Throwable ex) {
		return FailureResponse(ex.getMessage(), HttpStatus.CONFLICT);
	}

	public static APIResponse serverError(Throwable ex) {
		return FailureResponse(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
	}
	
	public static APIResponse badRequest(Throwable ex) {
		return FailureResponse(ex.getMessage(), HttpStatus.BAD_REQUEST);
	}

	public static APIResponse retrieved(ObjectContainer<?,?> oc) {
		return SuccessResponse(null, oc.asResponse(), HttpStatus.OK);
	}
	
	public static <D extends SpecificationDocument, F extends Response, M extends ResponseModifier>	
		APIResponse retrieved(ObjectContainer<D, F> oc, M mr) {
		
		return SuccessResponse(null, toResponse(oc, mr), HttpStatus.OK);
	}

	public static APIResponse result(Object object) {
		return SuccessResponse(null, object, HttpStatus.OK);
	}

	public static APIResponse result(ListResult<?> lr) {
		return SuccessResponse(null, lr.getData(), lr.getMetadata(), lr.getPagination(), HttpStatus.OK);
	}

	public static <D extends ContentDocument, F extends Response> APIResponse contentRetrieved(ObjectContainer<D,F> oc) {
		return SuccessResponse(null, oc.getObject().getContent(), HttpStatus.OK);
	}
	
	public static APIResponse selectSigninRole(SelectRoleResponse roles) {
		return SuccessResponse("You must select a signin role.", roles, HttpStatus.OK);
	}

	public static <D extends MemberDocument<?>, F extends Response, M extends ResponseModifier, X extends MemberContainer<D,F,?> & MultipleResponseContainer<D,F,M> >
		APIResponse memberAdded(X oc, ObjectContainer<?,?> member, M mr) {
		return SuccessResponse("The " + className(member.getClass()) + " has been added.", oc.asResponse(mr), HttpStatus.OK);
	}

	public static APIResponse memberAdded(MemberContainer<?,?,?> oc, ObjectContainer<?,?> member) {
		return SuccessResponse("The " + className(member.getClass()) + " has been added.", oc.asResponse(), HttpStatus.OK);
	}

	public static APIResponse memberAdded(ObjectContainer<?,?> member) {
		return SuccessResponse("The " + className(member.getClass()) + " has been added.", HttpStatus.OK);
	}

	public static APIResponse memberRemoved(ObjectContainer<?,?> oc) {
		return SuccessResponse("The " + className(oc.getClass()) + " has been removed.", HttpStatus.OK);
	}

	public static APIResponse alreadyMember(MemberContainer<?,?,?> oc, ObjectContainer<?,?> member) {
		return FailureResponse("The " + className(member.getClass()) + " is already member of the " + className(oc.getClass()) + ".", HttpStatus.CONFLICT);
	}

	public static APIResponse alreadyMember(ObjectContainer<?,?> member, InverseMemberContainer<?,?,?> oc) {
		return FailureResponse("The " + className(oc.getClass()) + " already contains " + className(member.getClass()) + ".", HttpStatus.CONFLICT);
	}

	public static APIResponse notMember(MemberContainer<?,?,?> oc, ObjectContainer<?,?> member) {
		return FailureResponse("The " + className(member.getClass()) + " is not a member of the " + className(oc.getClass()) + ".", HttpStatus.CONFLICT);
	}

	public static APIResponse notMember(ObjectContainer<?,?> member, InverseMemberContainer<?,?,?> oc) {
		return FailureResponse("The " + className(oc.getClass()) + " does not contain " + className(member.getClass()) + ".", HttpStatus.CONFLICT);
	}

	public static <M extends ResponseModifier> APIResponse assigned(ObjectContainer<?,?> oc1, ObjectContainer<?,?> oc2, M rm) {
		return SuccessResponse("The " + className(oc1.getClass()) + " has been assigned to the " + className(oc2.getClass()) + ".", toResponse(oc1, rm), HttpStatus.OK);
	}

	private static <B extends Response, M extends ResponseModifier> B toResponse(ObjectContainer<?,B> oc, M rm) {
		if (rm == null) {
			return oc.asResponse();
		} else {
			return ((MultipleResponseContainer<?,B,M>)oc).asResponse(rm);
		}
		
	}
	public static <B extends Response, M extends ResponseModifier> APIResponse assigned(List<ObjectContainer<?,B>> oc1, ObjectContainer<?,?> oc2, M rm) {
		if (oc1.size() > 0) {
			List<B> res = new ArrayList<>();
			
			for (ObjectContainer<?,B> oc : oc1) {
				res.add(toResponse(oc, rm));
			}
		
			return SuccessResponse(res.size() + " " + className(oc1.get(0).getClass()) + (res.size() == 1 ? " has " : "s have ") + "been assigned to the " + className(oc2.getClass()) + ".", res, HttpStatus.OK);
		} else {
			return FailureResponse("Nothing could be assigned to the " + className(oc2.getClass()) + ".", HttpStatus.BAD_REQUEST);
		}
	}

	public static <M extends ResponseModifier> APIResponse unassigned(ObjectContainer<?,?> oc1, ObjectContainer<?,?> oc2, M rm) {
		return SuccessResponse("The " + className(oc1.getClass()) + " has been unassigned from the " + className(oc2.getClass()) + ".", toResponse(oc1, rm), HttpStatus.OK);
	}

	public static <B extends Response, M extends ResponseModifier> APIResponse unassigned(int size, Class<?> oc1Class, ObjectContainer<?,?> oc2) {
		if (size > 0) {
			return SuccessResponse(size + " " + className(oc1Class) + (size == 1 ? " has " : "s have ") + "been unassigned from the " + className(oc2.getClass()) + ".", HttpStatus.OK);
		} else {
			return FailureResponse("Nothing could be unassigned from the " + className(oc2.getClass()) + ".", HttpStatus.BAD_REQUEST);
		}		
	}

	public static APIResponse assigned(ObjectContainer<?,?> oc) {
		return SuccessResponse("The assignment to the " + className(oc.getClass()) + " was succesfull.", HttpStatus.OK);
	}

	public static APIResponse alreadyAssigned(ObjectContainer<?,?> oc1, ObjectContainer<?,?> oc2) {
		return FailureResponse("The " + className(oc1.getClass()) + " has already been assigned to the " + className(oc2.getClass()) + ".", HttpStatus.CONFLICT);
	}

	public static APIResponse notAssigned(ObjectContainer<?,?> oc1, ObjectContainer<?,?> oc2) {
		return FailureResponse("The " + className(oc1.getClass()) + " is not assigned to the " + className(oc2.getClass()) + ".", HttpStatus.CONFLICT);
	}

	public static String className(Class<?> clazz) {
//		System.out.println(clazz.getSimpleName());
		
		if (clazz == AnnotatorContainer.class) {
			return "annotator";
		} else if (clazz == ComparatorContainer.class) {
			return "comparator";
		} else if (clazz == EmbedderContainer.class) {
			return "embedder";
		} else if (clazz == ClustererContainer.class) {
			return "clusterer";
		} else if (clazz == MappingContainer.class) {
			return "mapping";
		} else if (clazz == FileContainer.class) {
			return "file";
		} else if (clazz == DatasetContainer.class) {
			return "dataset";
		} else if (clazz == PagedAnnotationValidationContainer.class) {
			return "paged annotation validation";
		} else if (clazz == FilterAnnotationValidationContainer.class) {
			return "filter annotation validation";
		} else if (clazz == IndexContainer.class) {
			return "index";
		} else if (clazz == UserTaskContainer.class) {
			return "user task";
		} else if (clazz == CampaignContainer.class) {
			return "campaign";
		} else if (clazz == ProjectContainer.class) {
			return "project";
		} else if (clazz == UserContainer.class) {
			return "user";
		} else {
			return "";
		}
	}

	public Pagination getPagination() {
		return pagination;
	}

	public void setPagination(Pagination pagination) {
		this.pagination = pagination;
	}

	public HttpStatus getHttpStatus() {
		return httpStatus;
	}

	public void setHttpStatus(HttpStatus httpStatus) {
		this.httpStatus = httpStatus;
	}

	public Object getMetadata() {
		return metadata;
	}

	public void setMetadata(Object metadata) {
		this.metadata = metadata;
	}
}
