package ac.software.semantic.controller;


import java.io.StringWriter;
import java.io.Writer;
import java.util.*;
import java.util.stream.Collectors;

import ac.software.semantic.model.*;
import ac.software.semantic.payload.*;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.service.*;
import edu.ntua.isci.ac.lod.vocabularies.VOIDVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.Vocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NsIterator;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.StdDateFormat;

import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.web.servlet.tags.Param;

@Tag(name = "Dataset API")
@RestController
@RequestMapping("/api/dataset")
public class APIDatasetController {

    @Autowired
 	private DatasetService datasetService;

    @Autowired
 	private SchemaService schemaService;

	@Autowired
	DatasetRepository datasetRepository;

//    @Autowired
// 	private IndexService indexService;

	@Autowired
	private ModelMapper modelMapper;

	@Autowired
	private MappingsService mappingsService;

	@Autowired
	private ApplicationEventPublisher applicationEventPublisher;

	@Autowired
	private TemplateService templateService;
	
    @Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfigurations;

	@Operation(summary = "Get datasets of logged in user.",
			description = "This endpoint contacts the mongo database. Returns datasets created by the user. This does not concern talking with virtuoso, nor published collections")
	@ApiResponses(value = {
			@io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "Operation Successful",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = DatasetResponse.class))}),
			@io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "Operation Failed",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = ErrorResponse.class))})
	})
    @GetMapping(value = "/getAll",
	        produces = "application/json")
	public ResponseEntity<?> getDatasets(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("type") String type)  {
		List<DatasetResponse> list = datasetService.getDatasets(currentUser, type);
		return ResponseEntity.ok(list);
	}       
    
    @PostMapping(value = "/create",
    		     produces = "application/json")
    public ResponseEntity<?> createDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
    		                               @RequestParam("name") String name, 
    		                               @RequestParam("type") String type, 
    		                               @RequestParam("typeUri") Optional<String> typeUri,
    		                               @RequestParam("asProperty") Optional<String> asProperty,
    		                               @RequestBody Optional<List<ResourceOption>> links)  {

    	Dataset dataset = datasetService.createDataset(currentUser, name, type, typeUri.isPresent() ? typeUri.get() : null,
    			                                                            asProperty.isPresent() ? asProperty.get() : null,
    			                                                            links.isPresent() ? links.get() : null, ImportType.CUSTOM);
    	
    	DatasetResponse res = modelMapper.dataset2DatasetResponse(virtuosoConfigurations.values(), dataset);

    	return ResponseEntity.ok(res);
	}

	@PostMapping(value = "/create/predefined",
			produces = "application/json")
	public ResponseEntity<?> createEuropeanaDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
													@RequestParam("datasetName") String name,
													@RequestParam("importAs") String importAs
													) {

		ImportType importType = ImportType.get(importAs);

		// Step 1: Create the empty predefinedImport dataset
		Dataset dataset = datasetService.createDataset(currentUser, name, "collection-dataset", "http://sw.islab.ntua.gr/semaspace/model/DataCollection",
				null, null, importType);

		// Europeana Import Specific code:
		if (importType.equals(ImportType.EUROPEANA)) {

			// Step 2: Metadata Mapping + Parameter Binding + Metadata Mapping Execution
			String d2rmlTemplate = templateService.getPredefinedImportTemplate(importType.toString(), "HEADER").getTemplateString();
			String mappingUuid = UUID.randomUUID().toString();
			String d2rmlResult = d2rmlTemplate.replace("{@@MAPPING_UUID@@}", mappingUuid).replace("{@@DATASET_UUID@@}", dataset.getUuid());

			// Need to replace stuff in d2rml template string DATASET_UUID, MAPPING_UUID
			MappingDocument docHead = mappingsService.create(currentUser, dataset.getId().toString(), "HEADER", "Metadata", d2rmlResult, Arrays.asList("COLLECTION"), null, mappingUuid);
			MappingInstance miHead = mappingsService.createParameterBinding(currentUser, docHead.getId().toString(), Arrays.asList(new ParameterBinding("COLLECTION", name)));
			mappingsService.executeMapping(currentUser, docHead.getId().toString(), miHead.getId().toString(), applicationEventPublisher);

			// Step 3: Create Mapping for query and collection

			d2rmlTemplate = templateService.getPredefinedImportTemplate(importType.toString(), "COLLECTION").getTemplateString();
			mappingUuid = UUID.randomUUID().toString();
			d2rmlResult = d2rmlTemplate.replace("{@@MAPPING_UUID@@}", mappingUuid);

			MappingDocument doc = mappingsService.create(currentUser, dataset.getId().toString(), "CONTENT", "COLLECTION", d2rmlResult, Arrays.asList("COLLECTION", "API_KEY"), null, mappingUuid);

			d2rmlTemplate = templateService.getPredefinedImportTemplate(importType.toString(), "QUERY").getTemplateString();
			mappingUuid = UUID.randomUUID().toString();
			d2rmlResult = d2rmlTemplate.replace("{@@MAPPING_UUID@@}", mappingUuid);

			doc = mappingsService.create(currentUser, dataset.getId().toString(), "CONTENT", "QUERY", d2rmlResult, Arrays.asList("QUERY", "API_KEY"), null, mappingUuid);

		}

		DatasetResponse res = modelMapper.dataset2DatasetResponse(virtuosoConfigurations.values(), dataset);

		return ResponseEntity.ok(res);
	}

    @PostMapping(value = "/update/{id}",
		     produces = "application/json")
	public ResponseEntity<?> updateDataset(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id,
			                               @RequestParam("name") String name,
			                               @RequestParam("type") String type,
			                               @RequestParam("typeUri") Optional<String> typeUri,
			                               @RequestParam("asProperty") Optional<String> asProperty,
			                               @RequestBody Optional<List<ResourceOption>> links)  {

		Dataset dataset = datasetService.updateDataset(currentUser, new ObjectId(id), name, type, typeUri.isPresent() ? typeUri.get() : null,
				                                                            asProperty.isPresent() ? asProperty.get() : null,
				                                                            links.isPresent() ? links.get() : null, applicationEventPublisher);
		if (dataset != null) {
			DatasetResponse res = modelMapper.dataset2DatasetResponse(virtuosoConfigurations.values(), dataset);

			return ResponseEntity.ok(res);
		} else {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
	}
    
    @DeleteMapping(value = "/delete/{id}",
		           produces = "application/json")
    public ResponseEntity<?> deleteDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

		boolean deleted = datasetService.deleteDataset(currentUser, id);
		
		if (deleted) {
			return ResponseEntity.ok(new ApiResponse(true, "Catalog deleted"));
		} else {
			return ResponseEntity.ok(new ApiResponse(false, "Current user is not owner of catalog"));
		}
	}
    

    
    @GetMapping(value = "/get/{id}",
	        produces = "application/json")
	public ResponseEntity<?> getDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
	
		Optional<DatasetResponse> dataset = datasetService.getDataset(currentUser, id);
		if (dataset.isPresent()) {
			return ResponseEntity.ok(dataset.get());
		} else {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
	}   
    
    @PostMapping(value = "/insert",
	             produces = "application/json")
	public ResponseEntity<?> insertDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("id") String id, @RequestParam("toId") String toId)  {
	
		boolean inserted = datasetService.insert(currentUser, id, toId);
		if (inserted) {
			return ResponseEntity.ok(new ApiResponse(true,"")); 
		} else {
			return ResponseEntity.ok(new ApiResponse(false,"Target not found"));
		}
	}    
    
    @PostMapping(value = "/remove",
            produces = "application/json")
	public ResponseEntity<?> removeDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("id") String id, @RequestParam("fromId") String fromId)  {
	
		boolean inserted = datasetService.remove(currentUser, id, fromId);
		if (inserted) {
			return ResponseEntity.ok(new ApiResponse(true,"")); 
		} else {
			return ResponseEntity.ok(new ApiResponse(false,"Target not found"));
		}
	} 
    
//    @RequestMapping(value = "/checkMetadata/{id}")
// 	public ResponseEntity<?> checkMetadata(@CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
// 		
//		try {
//			AsyncUtils.supplyAsync(() -> mappingsService.executeMapping(currentUser, id, instanceId.isPresent() ? instanceId.get() : null, applicationEventPublisher));
//			
//			return new ResponseEntity<>(HttpStatus.ACCEPTED);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
// 	}

    
    @PostMapping(value = "/publish/{id}")
	public ResponseEntity<?> publishDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("triple-store") Optional<String> tripleStore, @PathVariable("id") String id, @RequestParam("visibility") String visibility)  {

		try {
//			DatasetMessage msg = datasetService.checkPublishVocabulary(currentUser, id);
			
//			if (msg != DatasetMessage.OK) {
//				return ResponseEntity.ok(new ApiResponse(false,msg.toString()));
//			} 

			final String virtuoso;
			if (tripleStore.isPresent()) {
				virtuoso = tripleStore.get();
			} else {
				virtuoso = virtuosoConfigurations.keySet().iterator().next();
			}
			
			AsyncUtils.supplyAsync(() -> datasetService.publish(currentUser, virtuoso, id, visibility.equals("public") ? 1:0, true, true, false))
			   .thenAccept(ok -> {
//				   System.out.println("SUCCESS");
				   DatasetResponse doc = datasetService.getDataset(currentUser, id).get();
				   
				   ObjectMapper mapper = new ObjectMapper();
				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
				    
				   NotificationObject no = new NotificationObject("publish", doc.getPublishState().toString(), id, null, doc.getPublishStartedAt(), doc.getPublishCompletedAt());
							
					try {
						SseEventBuilder sse = SseEmitter.event().name("dataset").data(mapper.writeValueAsBytes(no));
					
						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			   })
			   .exceptionally(ex -> { 
//				   	System.out.println("FAILURE");
					ObjectMapper mapper = new ObjectMapper();
					mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
				    
					NotificationObject no = new NotificationObject("publish", DatasetState.PUBLISHING_FAILED.toString(), id, null, null, null);
							
					try {
						SseEventBuilder sse = SseEmitter.event().name("dataset").data(mapper.writeValueAsBytes(no));
					
						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				   
				   ex.printStackTrace(); 
				   return null; 
				});			   
			
//			System.out.println("PUBLISHING ACCEPTED");
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		
	} 
    
    @PostMapping(value = "/publishUnpublishedContent/{id}")
 	public ResponseEntity<?> publishUnpublishedContent(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,  @PathVariable("id") String id)  {

 		try {
 			
 			Optional<Dataset> dopt = datasetRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
 			
 			if (!dopt.isPresent()) {
 				return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
 			}
 			
 			Dataset dataset = dopt.get();

 			PublishState ps = null;
 			String virtuoso = null;

 			for (VirtuosoConfiguration vc : virtuosoConfigurations.values()) {
 				ps = dataset.checkPublishState(vc.getId());
 				if (ps != null) {
 					virtuoso = vc.getName();
 					break;
 				}
 			}
 			final String virt = virtuoso;

 			AsyncUtils.supplyAsync(() -> datasetService.publish(currentUser, virt, id, -1, false, true, true))
 			   .thenAccept(ok -> {
// 				   System.out.println("SUCCESS");
 				   DatasetResponse doc = datasetService.getDataset(currentUser, id).get();
 				   
 				   ObjectMapper mapper = new ObjectMapper();
 				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
 				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
 				    
 				   NotificationObject no = new NotificationObject("publish", doc.getPublishState().toString(), id, null, doc.getPublishStartedAt(), doc.getPublishCompletedAt());
 							
 					try {
 						SseEventBuilder sse = SseEmitter.event().name("dataset").data(mapper.writeValueAsBytes(no));
 					
 						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
 					} catch (JsonProcessingException e) {
 						// TODO Auto-generated catch block
 						e.printStackTrace();
 					}
 			   })
 			   .exceptionally(ex -> { 
// 				   	System.out.println("FAILURE");
 					ObjectMapper mapper = new ObjectMapper();
 					mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
 				    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
 				    
 					NotificationObject no = new NotificationObject("publish", DatasetState.PUBLISHING_FAILED.toString(), id, null, null, null);
 							
 					try {
 						SseEventBuilder sse = SseEmitter.event().name("dataset").data(mapper.writeValueAsBytes(no));
 					
 						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
 					} catch (JsonProcessingException e) {
 						// TODO Auto-generated catch block
 						e.printStackTrace();
 					}
 				   
 				   ex.printStackTrace(); 
 				   return null; 
 				});			   
 			
// 			System.out.println("PUBLISHING ACCEPTED");
 			return new ResponseEntity<>(HttpStatus.ACCEPTED);
 		} catch (Exception e) {
 			e.printStackTrace();
 		}

 		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
 		
 	} 
    
    @PostMapping(value = "/republishMetadata/{id}")
 	public ResponseEntity<?> republishMetadata(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

 		try {
// 			DatasetMessage msg = datasetService.checkPublishVocabulary(currentUser, id);
 			
// 			if (msg != DatasetMessage.OK) {
// 				return ResponseEntity.ok(new ApiResponse(false,msg.toString()));
// 			} 

 			AsyncUtils.supplyAsync(() -> datasetService.republishMetadata(currentUser, id))
 			   .thenAccept(ok -> {
// 				   System.out.println("SUCCESS");
 				   DatasetResponse doc = datasetService.getDataset(currentUser, id).get();
 				   
 				   ObjectMapper mapper = new ObjectMapper();
 				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
 				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
 				    
 				   NotificationObject no = new NotificationObject("publish", doc.getPublishState().toString(), id, null, doc.getPublishStartedAt(), doc.getPublishCompletedAt());
 							
 					try {
 						SseEventBuilder sse = SseEmitter.event().name("dataset").data(mapper.writeValueAsBytes(no));
 					
 						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
 					} catch (JsonProcessingException e) {
 						// TODO Auto-generated catch block
 						e.printStackTrace();
 					}
 			   })
 			   .exceptionally(ex -> { 
// 				   	System.out.println("FAILURE");
 					ObjectMapper mapper = new ObjectMapper();
 					mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
 				    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
 				    
 					NotificationObject no = new NotificationObject("publish", DatasetState.PUBLISHING_FAILED.toString(), id, null, null, null);
 							
 					try {
 						SseEventBuilder sse = SseEmitter.event().name("dataset").data(mapper.writeValueAsBytes(no));
 					
 						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
 					} catch (JsonProcessingException e) {
 						// TODO Auto-generated catch block
 						e.printStackTrace();
 					}
 				   
 				   ex.printStackTrace(); 
 				   return null; 
 				});			   
 			
// 			System.out.println("PUBLISHING ACCEPTED");
 			return new ResponseEntity<>(HttpStatus.ACCEPTED);
 		} catch (Exception e) {
 			e.printStackTrace();
 		}

 		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
 		
 	} 
    
    @PostMapping(value = "/flipVisibility/{id}")
  	public ResponseEntity<?> flipVisibility(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,  @PathVariable("id") String id)  {

  		try {
//  			DatasetMessage msg = datasetService.checkPublishVocabulary(currentUser, id);
  			
//  			if (msg != DatasetMessage.OK) {
//  				return ResponseEntity.ok(new ApiResponse(false,msg.toString()));
//  			} 

  			AsyncUtils.supplyAsync(() -> datasetService.flipVisibility(currentUser, id))
  			   .thenAccept(ok -> {
//  				   System.out.println("SUCCESS");
  				   DatasetResponse doc = datasetService.getDataset(currentUser, id).get();
  				   
  				   ObjectMapper mapper = new ObjectMapper();
  				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
  				    
  				   NotificationObject no = new NotificationObject("publish", doc.getPublishState().toString(), id, null, doc.getPublishStartedAt(), doc.getPublishCompletedAt());
  							
  					try {
  						SseEventBuilder sse = SseEmitter.event().name("dataset").data(mapper.writeValueAsBytes(no));
  					
  						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
  					} catch (JsonProcessingException e) {
  						// TODO Auto-generated catch block
  						e.printStackTrace();
  					}
  			   })
  			   .exceptionally(ex -> { 
//  				   	System.out.println("FAILURE");
  					ObjectMapper mapper = new ObjectMapper();
  					mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  				    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
  				    
  					NotificationObject no = new NotificationObject("publish", DatasetState.PUBLISHING_FAILED.toString(), id, null, null, null);
  							
  					try {
  						SseEventBuilder sse = SseEmitter.event().name("dataset").data(mapper.writeValueAsBytes(no));
  					
  						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
  					} catch (JsonProcessingException e) {
  						// TODO Auto-generated catch block
  						e.printStackTrace();
  					}
  				   
  				   ex.printStackTrace(); 
  				   return null; 
  				});			   
  			
//  			System.out.println("PUBLISHING ACCEPTED");
  			return new ResponseEntity<>(HttpStatus.ACCEPTED);
  		} catch (Exception e) {
  			e.printStackTrace();
  		}

  		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
  		
  	}     
    
    @PostMapping(value = "/unpublish/{id}")
	public ResponseEntity<?> unpublishDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
		
		try {
			AsyncUtils.supplyAsync(() -> datasetService.unpublish(currentUser, id, true, true))
			   .exceptionally(ex -> { ex.printStackTrace(); return false; })
			   .thenAccept(ok -> {
				   ObjectMapper mapper = new ObjectMapper();
				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
				    
				   NotificationObject no = new NotificationObject("publish", DatasetState.UNPUBLISHED.toString(), id, null, null, null);
							
					try {
						SseEventBuilder sse = SseEmitter.event().name("dataset").data(mapper.writeValueAsBytes(no));
					
						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			   });
			
//			System.out.println("UNPUBLISHING ACCEPTED");
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		
    } 
    
    @PostMapping(value = "/index/{id}")
	public ResponseEntity<?> indexDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
		
		try {
			AsyncUtils.supplyAsync(() -> datasetService.indexDataset(currentUser, id))
			   .exceptionally(ex -> { 
//				   	System.out.println("FAILURE");
					ObjectMapper mapper = new ObjectMapper();
					mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
				    
					NotificationObject no = new NotificationObject("index", IndexingState.INDEXING_FAILED.toString(), id, null, null, null);
							
					try {
						SseEventBuilder sse = SseEmitter.event().name("dataset").data(mapper.writeValueAsBytes(no));
					
						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				   
				   ex.printStackTrace(); 
				   return false; 
				})
			   .thenAccept(ok -> {
				   DatasetResponse doc = datasetService.getDataset(currentUser, id).get();
				   
				   ObjectMapper mapper = new ObjectMapper();
				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
				    
				   NotificationObject no = new NotificationObject("index", IndexingState.INDEXED.toString(), id, null, doc.getPublishStartedAt(), doc.getPublishCompletedAt());
							
					try {
						SseEventBuilder sse = SseEmitter.event().name("dataset").data(mapper.writeValueAsBytes(no));
					
						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			   });
			
//			System.out.println("PUBLISHING ACCEPTED");
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
	}     
    
    @PostMapping(value = "/unindex/{id}")
	public ResponseEntity<?> unindexDataset(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {
		
		try {
			AsyncUtils.supplyAsync(() -> datasetService.unindexDataset(currentUser, id))
			   .exceptionally(ex -> { ex.printStackTrace(); return false; })
			   .thenAccept(ok -> {
				   ObjectMapper mapper = new ObjectMapper();
				   mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				   mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
				    
				   NotificationObject no = new NotificationObject("index", IndexingState.NOT_INDEXED.toString(), id, null, null, null);
							
					try {
						SseEventBuilder sse = SseEmitter.event().name("dataset").data(mapper.writeValueAsBytes(no));
					
						applicationEventPublisher.publishEvent(new SseApplicationEvent(this, sse));
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			   });
			
//			System.out.println("UNPUBLISHING ACCEPTED");
			return new ResponseEntity<>(HttpStatus.ACCEPTED);
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		
    } 

    @GetMapping(value = "/triple-stores",
	        produces = "application/json")
	public ResponseEntity<?> getDatabases(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser)  {
    	ObjectMapper mapper = new ObjectMapper();
    	ArrayNode array = mapper.createArrayNode();
    	
    	List<String> vcNames = virtuosoConfigurations.values().stream().map(vc -> vc.getName()).collect(Collectors.toList());
    	Collections.sort(vcNames);
    	
    	for (String name : vcNames) {
    		array.add(name);
    	}
    	
		return ResponseEntity.ok(array);
	}       

    @GetMapping(value = "/schema/{id}")
    public ResponseEntity<?> schema(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id, @RequestParam(defaultValue = "ttl", name = "format") String format)  {

		Optional<DatasetResponse> dataset = datasetService.getDataset(currentUser, id);
		if (dataset.isPresent()) {
			Model model = schemaService.readSchema(SEMAVocabulary.getDataset(dataset.get().getUuid()).toString());
			Writer sw = new StringWriter();
			model.write(sw, format) ;

			return ResponseEntity.ok(sw.toString());
		} else {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}

    }
}
