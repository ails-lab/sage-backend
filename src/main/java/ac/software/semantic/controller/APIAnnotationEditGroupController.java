package ac.software.semantic.controller;

import java.io.BufferedOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotationEditGroupSearch;
import ac.software.semantic.model.AnnotationEditGroupSearchField;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.constants.type.AnnotationValidationMode;
import ac.software.semantic.model.constants.type.SerializationType;
import ac.software.semantic.payload.request.AnnotationEditGroupUpdateRequest;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.AnnotationEditGroupResponse;
import ac.software.semantic.payload.response.AnnotationEditResponse;
import ac.software.semantic.payload.response.DatasetResponse;
import ac.software.semantic.payload.response.FilterCheckResponse;
import ac.software.semantic.payload.response.FilterFieldCheck;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.repository.core.AnnotationEditGroupRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotationEditGroupService;
import ac.software.semantic.service.AnnotationEditGroupService.AnnotationEditGroupContainer;
import ac.software.semantic.service.AnnotationEditService;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.exception.ContainerNotFoundException;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Annotation Edit Group API")
@RestController
@RequestMapping("/api/annotation-edit-group")
public class APIAnnotationEditGroupController {
    
	private Logger logger = LoggerFactory.getLogger(APIAnnotationEditGroupController.class);
	
	@Autowired
	private AnnotationEditGroupRepository aegRepository;
	
	@Autowired
    private AnnotationEditService annotationEditService;

	@Autowired
	private AnnotationEditGroupService aegService;

	@Autowired
    private DatasetService datasetService;

	@Autowired
	private APIUtils apiUtils;
	
	@GetMapping(value = "/get-all")
	public ResponseEntity<APIResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam ObjectId datasetId)  {
		try {
			ObjectContainer<Dataset,DatasetResponse> dc = apiUtils.exists(currentUser, new SimpleObjectIdentifier(datasetId), datasetService);
	    	
	    	List<Response> res = new ArrayList<>();
	    	for (AnnotationEditGroup aeg : aegService.getAnnotationEditGroups(dc.getObject())) {
	    		res.add(aegService.getContainer(currentUser, aeg, dc.getObject()).asResponse());
			}
	    	
			return APIResponse.result(res).toResponseEntity();
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}				
	}
	
	@GetMapping(value = "/get-all-my")
	public ResponseEntity<APIResponse> getAllByUser(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam ObjectId datasetId)  {
		return APIResponse.result(apiUtils.getAllByUser(currentUser, currentUser, datasetId != null ? Arrays.asList(new ObjectId[] {datasetId}) : null, aegService, datasetService)).toResponseEntity();
	}

    @PostMapping(value = "/update/{id}")
    public ResponseEntity<APIResponse> update(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestBody AnnotationEditGroupUpdateRequest ur)  {
    	return apiUtils.update(currentUser, new SimpleObjectIdentifier(id), ur, aegService).toResponseEntity();
    } 

    @PostMapping(value = "/view/{id}", produces = "application/json")
    public ResponseEntity<APIResponse> view(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestParam(value="page", defaultValue="1") int page, @RequestParam(value="mode", defaultValue="ALL") AnnotationValidationMode mode, @RequestBody(required = false) AnnotationEditGroupSearch options)  {

    	try {
    		AnnotationEditGroupContainer oc = (AnnotationEditGroupContainer)apiUtils.exists(currentUser, new SimpleObjectIdentifier(id), aegService);
	    	
//    		return APIResponse.result(aegService.view(currentUser, oc, mode, page)).toResponseEntity();
			return APIResponse.result(aegService.view2(currentUser, oc, mode, page, options)).toResponseEntity();
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}	
    }
    
    @PostMapping(value = "/commit-edits/{id}")
	public ResponseEntity<APIResponse> commit(@CurrentUser UserPrincipal currentUser, @PathVariable("id") ObjectId id, @RequestBody List<AnnotationEditResponse> edits)  {

    	try {
	    	ObjectContainer<AnnotationEditGroup, AnnotationEditGroupResponse> oc = apiUtils.exists(currentUser, new SimpleObjectIdentifier(id), aegService);
	    	
	 		for (AnnotationEditResponse vad : edits) { 
	 			annotationEditService.processEdit(currentUser, oc.getObject(), vad);
	 		}
		 	
			return APIResponse.ok().toResponseEntity();
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}					 	
	}

	@GetMapping(value = "/export-annotations-validations/{id}")
	public ResponseEntity<StreamingResponseBody> downloadAnnotationValues(@CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, 
			@RequestParam(required = false, defaultValue = "JSON-LD") String serialization, 
			@RequestParam(required = false, defaultValue = "false") boolean onlyReviewed, 
			@RequestParam(required = false, defaultValue = "true") boolean onlyNonRejected,
			@RequestParam(required = false, defaultValue = "true") boolean onlyFresh,
			@RequestParam(required = false, defaultValue = "true") boolean created,
			@RequestParam(required = false, defaultValue = "true") boolean creator,
			@RequestParam(required = false, defaultValue = "true") boolean score,
			@RequestParam(required = false, defaultValue = "true") boolean scope, 
			@RequestParam(required = false, defaultValue = "true") boolean selector, 
			@RequestParam(required = false, defaultValue = "TGZ") String archive
			) {

		try {
			
			Optional<AnnotationEditGroup> aegOpt = aegRepository.findById(id);
			
			if (!aegOpt.isPresent()) {
				return ResponseEntity.notFound().build();
			}
			
			AnnotationEditGroup aeg = aegOpt.get();
			
			if (archive.equalsIgnoreCase("zip")) {
				
	    		HttpHeaders headers = new HttpHeaders();
	    	    headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
	    	    headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=annotations_" + aeg.getUuid() + ".zip");

	    	    StreamingResponseBody stream = outputStream -> {
					try (ZipOutputStream zos = new ZipOutputStream(outputStream)) {
						logger.info("Exporting annotations for aeg " + aeg.getId() + " started.");
						aegService.exportAnnotations(id, SerializationType.get(serialization), onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, zos) ;
						zos.finish();
					
					} catch (Exception e) {
						logger.info("Exporting annotations for aeg " + aeg.getId() + " failed.");
						e.printStackTrace();
					}
					logger.info("Exporting annotations for aeg " + aeg.getId() + " completed.");
				};
				
	    	    return ResponseEntity.ok()
	    	            .headers(headers)
	    	            .contentType(MediaType.APPLICATION_OCTET_STREAM)
	    	            .body(stream);
	    	    
			} else if (archive.equalsIgnoreCase("tgz"))	{

	    	    HttpHeaders headers = new HttpHeaders();
	    	    headers.add(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION);
	    	    headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=annotations_" + aegOpt.get().getUuid() + ".tgz");
	    	    
	    	    StreamingResponseBody stream = outputStream -> {
	    			try (
	    					BufferedOutputStream buffOut = new BufferedOutputStream(outputStream);
	    					GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(buffOut);
//	    					GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(outputStream);
	    					TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {
	    				logger.info("Exporting annotations for aeg " + aeg.getId() + " started.");
						aegService.exportAnnotations(id, SerializationType.get(serialization), onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, tOut) ;
					} catch (Exception e) {
						logger.info("Exporting annotations for aeg " + aeg.getId() + " failed.");
						e.printStackTrace();
					}	    				
	    			logger.info("Exporting annotations for aeg " + aeg.getId() + " completed.");
				};
				
	    	    return ResponseEntity.ok()
	    	            .headers(headers)
	    	            .contentType(MediaType.APPLICATION_OCTET_STREAM)
	    	            .body(stream);
			} else {
				return ResponseEntity.notFound().build();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	
	}


    @PostMapping(value = "/check-filter", produces = "application/json")
    public ResponseEntity<APIResponse> check(@CurrentUser UserPrincipal currentUser, @RequestBody AnnotationEditGroupSearch options)  {

    	try {
        	FilterCheckResponse fcr = new FilterCheckResponse();

    		for (AnnotationEditGroupSearchField field : options.getFields()) {
        		String sparql = "SELECT * WHERE { ?p <http://www.test.org/test> ?r" + field.getField() + " . VALUES ?r" + field.getField() + " { " + field.getValue() + " } }";
    			
        		try {
        			QueryFactory.create(sparql, Syntax.syntaxSPARQL_11);
        			
        			fcr.addField(new FilterFieldCheck(field.getField(), true));
        		} catch (Exception ex) {
        			fcr.addField(new FilterFieldCheck(field.getField(), false));
        		}
    		}
    		
    		return APIResponse.result(fcr).toResponseEntity();
    		
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
    }
    
}
