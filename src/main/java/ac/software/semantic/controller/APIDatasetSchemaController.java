package ac.software.semantic.controller;

import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.swagger.v3.oas.annotations.Parameter;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fasterxml.jackson.databind.ObjectMapper;

import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.Pagination;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.RDFMediaType;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.payload.ByteData;
import ac.software.semantic.payload.ListResult;
import ac.software.semantic.payload.PropertyDoubleValue;
import ac.software.semantic.payload.PropertyValue;
import ac.software.semantic.payload.ValueAnnotationReference;
import ac.software.semantic.payload.ValueResponseContainer;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.ClassStructureResponse;
import ac.software.semantic.payload.response.ValueResponse;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.EmbedderDocumentRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.service.SchemaService;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.SchemaService.ClassStructure;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.exception.ContainerNotFoundException;
import ac.software.semantic.vocs.SACCVocabulary;
import ac.software.semantic.vocs.SEMAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.VOIDVocabulary;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Dataset Schema API")

@RestController
@RequestMapping("/api/dataset")
public class APIDatasetSchemaController {

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	private DatasetService datasetService;

	@Autowired
	private APIUtils apiUtils;
	
	@Autowired
	private SchemaService schemaService;
	
    @Autowired
    private EmbedderDocumentRepository embedderRepository;
	

    @GetMapping(value = "/rdf-schema/{id}",
	        produces = {RDFMediaType.APPLICATION_JSONLD_VALUE, RDFMediaType.APPLICATION_TRIG_VALUE, RDFMediaType.APPLICATION_N_TRIPLES_VALUE, RDFMediaType.APPLICATION_RDF_XML_VALUE, RDFMediaType.TEXT_TURTLE_VALUE })
	public ResponseEntity<?> rdfschema(@RequestHeader("Content-Type") String contentType, @Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		try {
			
			ObjectContainer<Dataset,?> oc = apiUtils.exists(currentUser, new SimpleObjectIdentifier(id), datasetService);
	 	
			Model model = schemaService.readSchema(oc.getObject());
	
			final HttpHeaders httpHeaders = new HttpHeaders();
		    httpHeaders.setContentType(RDFMediaType.getMediaType(contentType));
	
			try (Writer sw = new StringWriter()) {
				schemaService.serializeSchema(sw, model, RDFMediaType.getFormat(contentType));
				return new ResponseEntity(sw.toString(), httpHeaders, HttpStatus.OK);
			}
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}
	
    @GetMapping(value = "/schema/{id}",
	            produces = {MediaType.APPLICATION_JSON_VALUE })
	public ResponseEntity<?> schema(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id)  {
		try {
			
			ObjectContainer<Dataset,?> oc = apiUtils.exists(currentUser, new SimpleObjectIdentifier(id), datasetService);
	 	
			Model model = schemaService.readSchema(oc.getObject(), VOIDVocabulary.PREFIX);
			Object r  = schemaService.jsonSerializeSchema(model);
			return APIResponse.result(r).toResponseEntity();
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}	
    
    @GetMapping(value = "/schema/{id}/classes")
    public ResponseEntity<?> schemaClasses(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam(required = false, defaultValue = "false") boolean embedders)  {

    	try {
    		ObjectContainer<Dataset,?> oc = apiUtils.exists(currentUser, new SimpleObjectIdentifier(id), datasetService);
			
			Dataset dataset = oc.getObject();
			
			List<ClassStructure> tcs = schemaService.readTopClasses(dataset);
			
			List<ClassStructureResponse> cs = new ArrayList<>();
			for (ClassStructure css : tcs) {
				ClassStructureResponse csr = ClassStructureResponse.createFrom(css, false);
				
				if (embedders) {
					for (EmbedderDocument edoc : embedderRepository.findByDatasetUuidAndOnClass(dataset.getUuid(), csr.getClazz())) {
						csr.addEmbedder(edoc.getId().toString(), edoc.getEmbedder());
					}
				}
				
				cs.add(csr);

			}
				
			return ResponseEntity.ok(cs);
		
    	} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
    }
    
	@PostMapping(value = "/schema/{id}/list-property-values")
	public ResponseEntity<APIResponse> propertyValues(@CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam("mode") String mode,
			                                @RequestBody List<PathElement> path,
			                                @RequestParam(required = false) Integer page, @RequestParam(defaultValue = "${api.pagination.size}") int size) {
		try {
			ObjectContainer<Dataset,?> oc = apiUtils.exists(currentUser, new SimpleObjectIdentifier(id), datasetService);
	
			ValueResponseContainer<ValueResponse> vrc = schemaService.getPropertyValues(oc.getObject(), path, mode, page, size);
			
			Pagination pg = null;
			if (page != null) {
				pg = new Pagination();
				pg.setCurrentPage(page);
				pg.setCurrentElements(vrc.getValues().size());
				pg.setTotalPages((int)Math.ceil(vrc.getDistinctTotalCount()/(double)size));
				pg.setTotalElements(vrc.getDistinctTotalCount());
			}
			
			ListResult<ValueResponse> list = new ListResult<>(vrc.getValues(), pg);
			vrc.setValues(null);
			list.setMetadata(vrc);
			
			return APIResponse.result(list).toResponseEntity();
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}
	
	@PostMapping(value = "/schema/{id}/list-items-for-property-value")
	public ResponseEntity<APIResponse> itemsForPropertyValue(@CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, 
			                                @RequestBody PropertyValue pv,
			                                @RequestParam(required = false) Integer page, @RequestParam(defaultValue = "${api.pagination.size}") int size) {
		try {
			ObjectContainer<Dataset,?> oc = apiUtils.exists(currentUser, new SimpleObjectIdentifier(id), datasetService);
	
			ValueResponseContainer<ValueResponse> vrc = schemaService.getItemsForPropertyValue(oc.getObject(), pv, page, size);
			
			Pagination pg = null;
			if (page != null) {
				pg = new Pagination();
				pg.setCurrentPage(page);
				pg.setCurrentElements(vrc.getValues().size());
				pg.setTotalPages((int)Math.ceil(vrc.getDistinctTotalCount()/(double)size));
				pg.setTotalElements(vrc.getDistinctTotalCount());
			}
			
			ListResult<ValueResponse> list = new ListResult<>(vrc.getValues(), pg);
			vrc.setValues(null);
			list.setMetadata(vrc);
			
			return APIResponse.result(list).toResponseEntity();
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}
	
	@PostMapping(value = "/schema/{id}/list-properties-for-target")
	public ResponseEntity<APIResponse> propertyValues(@CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, 
			                                @RequestBody PropertyDoubleValue pv) {
		try {
			ObjectContainer<Dataset,?> oc = apiUtils.exists(currentUser, new SimpleObjectIdentifier(id), datasetService);
	
			List<ValueAnnotationReference> res = schemaService.getPropertyValuesForItem(oc.getObject(), pv);
			
			return APIResponse.result(res).toResponseEntity();
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}
	
	@PostMapping(value = "/schema/{id}/download-property-values")
	public ResponseEntity<StreamingResponseBody> downloadPropertyValues(@CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id, @RequestParam("mode") String mode,
	 		                                                            @RequestBody List<PathElement> path) {
		try {
			ObjectContainer<Dataset,?> oc = apiUtils.exists(currentUser, new SimpleObjectIdentifier(id), datasetService);
	
			ByteData bd = schemaService.downloadPropertyValues(oc.getObject(), path, mode);
	
			return apiUtils.downloadFile(bd.getData(), bd.getFileName());
			
		} catch (ContainerNotFoundException ex) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
	}
	
	@GetMapping(value = "/schema/{id}/metadata", produces = "application/json")
	public ResponseEntity<APIResponse> metadata(@CurrentUser UserPrincipal currentUser, @PathVariable ObjectId id) throws Exception {

		try {

			ObjectContainer<Dataset,?> oc = apiUtils.exists(currentUser, new SimpleObjectIdentifier(id), datasetService);
	
			Model model = schemaService.readMetadata(oc.getObject());
			Object r  = schemaService.jsonSerializeMetadata(model);
			
			return APIResponse.result(r).toResponseEntity();
			
		} catch (ContainerNotFoundException ex) {
			return APIResponse.notFound(ex.getContainerClass()).toResponseEntity();	    	
		} catch (Exception ex) {
			ex.printStackTrace();
			return APIResponse.serverError(ex).toResponseEntity();
		}
	}
	

}
