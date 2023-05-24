package ac.software.semantic.service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.*;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.ThesaurusLoadStatus;
import ac.software.semantic.model.constants.UserRoleType;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.EmbedderIndexElement;
import ac.software.semantic.model.index.PropertyIndexElement;
import ac.software.semantic.model.state.CreateDistributionState;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.FileExecuteState;
import ac.software.semantic.model.state.FilePublishState;
import ac.software.semantic.model.state.IndexState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.PagedAnnotationValidationState;
import ac.software.semantic.model.state.PublishState;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import ac.software.semantic.payload.AnnotationEditGroupResponse;
import ac.software.semantic.payload.AnnotatorDocumentResponse;
import ac.software.semantic.payload.CampaignResponse;
import ac.software.semantic.payload.ClassIndexElementResponse;
import ac.software.semantic.payload.DataServiceResponse;
import ac.software.semantic.payload.DatabaseResponse;
import ac.software.semantic.payload.DatasetResponse;
import ac.software.semantic.payload.EmbedderDocumentResponse;
import ac.software.semantic.payload.EmbedderIndexElementResponse;
import ac.software.semantic.payload.FileResponse;
import ac.software.semantic.payload.FilterAnnotationValidationResponse;
import ac.software.semantic.payload.IndexDocumentResponse;
import ac.software.semantic.payload.IndexStructureResponse;
import ac.software.semantic.payload.MappingInstanceResponse;
import ac.software.semantic.payload.MappingResponse;
import ac.software.semantic.payload.PagedAnnotationValidationPageResponse;
import ac.software.semantic.payload.PagedAnnotationValidationResponse;
import ac.software.semantic.payload.PropertyIndexElementResponse;
import ac.software.semantic.payload.TemplateResponse;
import ac.software.semantic.payload.TripleStoreResponse;
import ac.software.semantic.payload.UserResponse;
import ac.software.semantic.payload.VocabularizerResponse;
import ac.software.semantic.payload.VocabularyResponse;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.security.UserPrincipal;

@Component
public class ModelMapper {

	@Autowired
	DatasetRepository datasetRepository;
	
    @Value("${mapping.uploaded-files.folder}")
    private String uploadsFolder;
    
    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("elastic-configurations")
    private ConfigurationContainer<ElasticConfiguration> elasticConfigurations;

    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;

	@Autowired
	FolderService folderService;
	
	@Autowired
    @Qualifier("rdf-vocabularies")
    private VocabularyContainer vocc;
	

    public ModelMapper() {  }
    
    public MappingResponse mapping2MappingResponse(TripleStoreConfiguration vc, MappingDocument doc, UserPrincipal currentUser)  {
    	MappingResponse response = new MappingResponse();
    	response.setId(doc.getId().toString());
    	response.setName(doc.getName());
//    	response.setD2RML(doc.getD2RML());
    	response.setDatasetId(doc.getDatasetId().toString());
    	response.setUuid(doc.getUuid());
    	response.setType(doc.getType().toString());
    	response.setParameters(doc.getParameters());
    	response.setFileName(doc.getFileName());
//    	response.setFileContents(doc.getFileContents());
    	response.setTemplate(doc.getTemplateId() != null);
    	response.setDataFiles(doc.getDataFiles());
    	
    	
    	List<MappingInstanceResponse> instances = new ArrayList<>();
    	for (MappingInstance inst : doc.getInstances()) {
    		instances.add(this.mappingInstance2MappingInstanceResponse(vc, inst));
    	}
    	
    	response.setInstances(instances);
    	
//    	response.setExecuteStartedAt(doc.getExecuteStartedAt());
//    	response.setExecuteCompletedAt(doc.getExecuteCompletedAt());
//    	response.setState(doc.getState());
    	
    	ArrayList<String> files = new ArrayList<>();
    	
//    	File f = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder + doc.getId().toString());
//    	if (f.exists() && f.isDirectory()) {
//    		for (File df : f.listFiles()) {
//    			if (df.isFile()) {
//    				files.add(df.getName());
//    			}
//    		}
//    	}
    	List<File> ff = folderService.getUploadedFiles(currentUser, doc);
    	if (ff != null) {
    		for (File f : ff) {
				files.add(f.getName());
			}
		}
    	
    	response.setFiles(files);

        return response;
    }
    
    public FileResponse file2FileResponse(TripleStoreConfiguration vc, Dataset dataset, FileDocument fd, UserPrincipal currentUser)  {
    	FileResponse response = new FileResponse();
    	response.setId(fd.getId().toString());
    	response.setName(fd.getName());
    	response.setDatasetId(fd.getDatasetId().toString());
    	response.setUuid(fd.getUuid());
    	
    	FileExecuteState es = fd.getExecute(); 
    	if (es != null) {
	    	response.setExecuteStartedAt(es.getExecuteStartedAt());
	    	response.setExecuteCompletedAt(es.getExecuteCompletedAt());
	    	response.setExecuteState(es.getExecuteState());
	    	response.setFileName(es.getFileName());
    	}
    	
    	
    	if (vc != null) { // if not published
    		FilePublishState ps = fd.checkPublishState(vc.getId());
	    	if (ps != null) {    	
		    	response.setPublishState(ps.getPublishState());
		    	FileExecuteState pes = ps.getExecute();
		    	
		    	if (pes != null) {
		    		if (pes.getDatabaseConfigurationId().equals(fileSystemConfiguration.getId())) {
		    			response.setPublishedFromCurrentFileSystem(true);
		    			
			    		response.setPublishStartedAt(ps.getPublishStartedAt());
			    		response.setPublishCompletedAt(ps.getPublishCompletedAt());
			    		
			    		if (es != null && pes.getExecuteStartedAt().equals(es.getExecuteStartedAt())) {
			    			response.setNewExecution(false);
			    		} else {
			    			response.setNewExecution(true);
			    		}
			    	} else {
			    	}
		        // for compatibility <--		    		
		    	} else { 
		    		response.setLegacy(true);
		    		response.setPublishStartedAt(ps.getPublishStartedAt());
		    		response.setPublishCompletedAt(ps.getPublishCompletedAt());
		        // for compatibility -->		    		
		    	}
	    	} 
    	}
    	
//    	ArrayList<String> files = new ArrayList<>();
//    	
//    	List<File> ff = folderService.getUploadedFiles(currentUser, dataset, fd);
//    	if (ff != null) {
//    		for (File f : ff) {
//				files.add(f.getName());
//			}
//		}
//    	
//    	response.setFiles(files);

        return response;
    }

    public DatasetResponse dataset2DatasetResponse(Dataset doc, Template template) {
    	return dataset2DatasetResponse(doc, template, null);
    }
    
    public DatasetResponse dataset2DatasetResponse(Dataset doc, Template template, ThesaurusLoadStatus tls) {
    	DatasetResponse response = new DatasetResponse();

    	response.setId(doc.getId().toString());
    	response.setName(doc.getName());
    	response.setScope(doc.getScope());
    	response.setType(doc.getDatasetType());
    	response.setTypeUri(doc.getTypeUri());
    	response.setUuid(doc.getUuid());
    	response.setIdentifier(doc.getIdentifier());
    	response.setPublik(doc.isPublik());
    	response.setLoadState(tls);
    	
//    	response.setImportType(doc.getImportType() != null ? doc.getImportType() : ImportType.custom);
    	if (template != null) {
    		response.setTemplate(template2TemplateResponse(template));
    	}

//    	if (doc.getTripleStoreId() != null) {
//    		for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) { // currently support only one publication site
//    			if (vc.getId().equals(doc.getTripleStoreId())) {
//    				response.setTripleStore(vc.getName());
//    				break;
//    			}
//    		}
//    	}
    	
    	for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) { // currently support only one publication site
	    	PublishState ps = doc.checkPublishState(vc.getId());
	    	if (ps != null) {
	    		response.setPublishDatabase(vc.getName());
		    	response.setPublishStartedAt(ps.getPublishStartedAt());
		    	response.setPublishCompletedAt(ps.getPublishCompletedAt());
		    	response.setPublishState(ps.getPublishState());
		    	response.setPublishMessages(ps.getMessages());
	    	}
	    	
	    	CreateDistributionState cds = doc.checkCreateDistributionState(fileSystemConfiguration.getId(), vc.getId());
	    	if (cds != null) {
		    	response.setCreateDistributionStartedAt(cds.getCreateDistributionStartedAt());
		    	response.setCreateDistributionCompletedAt(cds.getCreateDistributionCompletedAt());
		    	response.setCreateDistributionState(cds.getCreateDistributionState());
		    	response.setCreateDistributionMessages(cds.getMessages());
	    	}
    	}
//    	response.setSourceUri(doc.getSourceUri());
//    	response.setTargetUri(doc.getTargetUri());
    	response.setLinks(doc.getLinks());
    	for (ElasticConfiguration ec : elasticConfigurations.values()) { // currently support only one publication site
	    	IndexState is = doc.checkIndexState(ec.getId());
	    	if (is != null) {
	    		response.setIndexDatabase(ec.getName());
		    	response.setIndexStartedAt(is.getIndexStartedAt());
		    	response.setIndexCompletedAt(is.getIndexCompletedAt());
		    	response.setIndexState(is.getIndexState());
		    	response.setIndexMessages(is.getMessages());
	    	}
    	}
    	
    	if (doc.getDatasets() != null) {
	    	for (ObjectId did : doc.getDatasets()) {
	    		Optional<Dataset> op = datasetRepository.findById(did);
	    		if (op.isPresent()) {
	    			Dataset ds = op.get();
	    			DatasetResponse mr = new DatasetResponse();
	    			mr.setId(did.toString());
	    			mr.setName(ds.getName());
	    			mr.setScope(ds.getScope());
	    			mr.setType(ds.getDatasetType());
	    			mr.setTypeUri(ds.getTypeUri());
	    			mr.setUuid(ds.getUuid());
	    			mr.setIdentifier(ds.getIdentifier());
	    			for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) { // currently support only one publication site
		    	    	PublishState pss = ds.checkPublishState(vc.getId());
		    	    	if (pss != null) {    			
			    	    	mr.setPublishStartedAt(pss.getPublishStartedAt());
			    	    	mr.setPublishCompletedAt(pss.getPublishCompletedAt());
			    	    	mr.setPublishState(pss.getPublishState());
		    	    	}
	    			}
	//    	    	mr.setSourceUri(ds.getSourceUri());
	//    	    	mr.setTargetUri(ds.getTargetUri());
	    	    	mr.setLinks(ds.getLinks());
	    	    	for (ElasticConfiguration ec : elasticConfigurations.values()) { // currently support only one publication site
	    	    		IndexState iss = ds.checkIndexState(ec.getId());
		    	    	if (iss != null) {
			    	    	mr.setIndexStartedAt(iss.getIndexStartedAt());
			    	    	mr.setIndexCompletedAt(iss.getIndexCompletedAt());
			    	    	mr.setIndexState(iss.getIndexState());
		    	    	}
	    	    	}
	    			response.addDataset(mr);
	    		}
	    		
	    	}
    	}
    	
        return response;
    }
    
    public AnnotatorDocumentResponse annotator2AnnotatorResponse(TripleStoreConfiguration vc, AnnotatorDocument doc, AnnotationEditGroup aeg) {
    	AnnotatorDocumentResponse response = new AnnotatorDocumentResponse();
    	response.setId(doc.getId().toString());
    	response.setUuid(doc.getUuid());
    	response.setOnProperty(PathElement.onPathElementListAsStringListInverse(doc.getOnProperty(), vocc));
    	response.setAsProperty(doc.getAsProperty());
    	response.setAnnotator(doc.getAnnotator());
    	response.setThesaurus(doc.getThesaurus());
    	response.setParameters(doc.getParameters());
    	response.setPreprocess(doc.getPreprocess());
    	response.setVariant(doc.getVariant());
    	response.setDefaultTarget(vocc.arrayPrefixize(doc.getDefaultTarget()));
    	
    	MappingExecuteState es = doc.checkExecuteState(fileSystemConfiguration.getId());
    	if (es != null) {    	
	    	response.setExecuteState(es.getExecuteState());
	    	response.setExecuteStartedAt(es.getExecuteStartedAt());
	    	response.setExecuteCompletedAt(es.getExecuteCompletedAt());
	    	response.setCount(es.getCount());
	    	response.setExecuteMessages(es.getMessages());
	    	response.setExecuteMessages(es.getMessages());
	    	response.setD2rmlExecution(es.getD2rmlExecution());
	    	
    	}    	
    	
    	if (vc != null) { 
    		MappingPublishState ps = doc.checkPublishState(vc.getId());
	    	if (ps != null) {    	
	    		
		    	response.setPublishState(ps.getPublishState());
		    	MappingExecuteState pes = ps.getExecute();
		    	
		    	if (pes != null) {
		    		if (pes.getDatabaseConfigurationId().equals(fileSystemConfiguration.getId())) {
			    		response.setPublishedFromCurrentFileSystem(true);
			    		
			    		response.setPublishStartedAt(ps.getPublishStartedAt());
			    		response.setPublishCompletedAt(ps.getPublishCompletedAt());
			    		
			    		if (es != null && pes.getExecuteStartedAt().equals(es.getExecuteStartedAt())) {
			    			response.setNewExecution(false);
			    		} else {
			    			response.setNewExecution(true);
			    		}
			    	} else {
			    		
			    		response.setPublishStartedAt(ps.getPublishStartedAt());
			    		response.setPublishCompletedAt(ps.getPublishCompletedAt());
			    		
			    		response.setPublishedFromCurrentFileSystem(false);
			    	}
		        // for compatibility <--		    		
		    	} else { 
		    		response.setLegacy(true);
		    		response.setPublishStartedAt(ps.getPublishStartedAt());
		    		response.setPublishCompletedAt(ps.getPublishCompletedAt());
		        // for compatibility -->		    		
		    	}
	    	} else {
	    		response.setPublishState(DatasetState.UNPUBLISHED);
	    	} 
    	}

    	AnnotationEditGroupResponse aegr = new AnnotationEditGroupResponse();
    	aegr.setId(aeg.getId().toString());
    	aegr.setUuid(aeg.getUuid());
    	aegr.setDatasetUuid(aeg.getDatasetUuid());
    	aegr.setAsProperty(aeg.getAsProperty());
    	aegr.setOnProperty(PathElement.onPathElementListAsStringListInverse(aeg.getOnProperty(), null));
    	
    	response.setEditGroup(aegr);
    	
        return response;
    }
    
    public EmbedderDocumentResponse embedder2EmbedderResponse(TripleStoreConfiguration vc, EmbedderDocument doc) {
    	EmbedderDocumentResponse response = new EmbedderDocumentResponse();
    	response.setId(doc.getId().toString());
    	response.setUuid(doc.getUuid());
    	response.setElement(doc.getElement());
    	response.setEmbedder(doc.getEmbedder());
    	response.setVariant(doc.getVariant());
    	response.setOnClass(doc.getOnClass());
    	
    	MappingExecuteState es = doc.checkExecuteState(fileSystemConfiguration.getId());
    	if (es != null) {    	
	    	response.setExecuteState(es.getExecuteState());
	    	response.setExecuteStartedAt(es.getExecuteStartedAt());
	    	response.setExecuteCompletedAt(es.getExecuteCompletedAt());
	    	response.setExecuteMessages(es.getMessages());
	    	response.setCount(es.getCount());
	    	response.setExecuteMessages(es.getMessages());
	    	response.setD2rmlExecution(es.getD2rmlExecution());
	    	
    	}    	
    	
    	if (vc != null) { 
    		MappingPublishState ps = doc.checkPublishState(vc.getId());
	    	if (ps != null) {    	
	    		
		    	response.setPublishState(ps.getPublishState());
		    	MappingExecuteState pes = ps.getExecute();
		    	
		    	if (pes != null) {
		    		if (pes.getDatabaseConfigurationId().equals(fileSystemConfiguration.getId())) {
			    		response.setPublishedFromCurrentFileSystem(true);
			    		
			    		response.setPublishStartedAt(ps.getPublishStartedAt());
			    		response.setPublishCompletedAt(ps.getPublishCompletedAt());
			    		
			    		if (es != null && pes.getExecuteStartedAt().equals(es.getExecuteStartedAt())) {
			    			response.setNewExecution(false);
			    		} else {
			    			response.setNewExecution(true);
			    		}
			    	} else {
			    		
			    		response.setPublishStartedAt(ps.getPublishStartedAt());
			    		response.setPublishCompletedAt(ps.getPublishCompletedAt());
			    		
			    		response.setPublishedFromCurrentFileSystem(false);
			    	}
		        // for compatibility <--		    		
		    	} else { 
		    		response.setPublishStartedAt(ps.getPublishStartedAt());
		    		response.setPublishCompletedAt(ps.getPublishCompletedAt());
		        // for compatibility -->		    		
		    	}
	    	} else {
	    		response.setPublishState(DatasetState.UNPUBLISHED);
	    	} 
    	}
    	
        return response;
    }
    
    public MappingInstanceResponse mappingInstance2MappingInstanceResponse(TripleStoreConfiguration vc, MappingInstance doc) {
    	MappingInstanceResponse response = new MappingInstanceResponse();
    	response.setId(doc.getId().toString());
    	response.setDataFiles(doc.getDataFiles());
    	MappingExecuteState es = doc.checkExecuteState(fileSystemConfiguration.getId());
    	if (es != null) {    	
	    	response.setExecuteState(es.getExecuteState());
	    	response.setExecuteStartedAt(es.getExecuteStartedAt());
	    	response.setExecuteCompletedAt(es.getExecuteCompletedAt());
	    	response.setCount(es.getCount());
	    	response.setSparqlCount(es.getSparqlCount());
	    	response.setExecuteMessages(es.getMessages());
	    	response.setD2rmlExecution(es.getD2rmlExecution());
    	}

    	if (vc != null) { // if not published
    		MappingPublishState ps = doc.checkPublishState(vc.getId());
	    	if (ps != null) {    	
	    		
		    	response.setPublishState(ps.getPublishState());
		    	MappingExecuteState pes = ps.getExecute();
		    	
		    	if (pes != null) {
		    		if (pes.getDatabaseConfigurationId().equals(fileSystemConfiguration.getId())) {
			    		response.setPublishedFromCurrentFileSystem(true);
			    		
			    		response.setPublishStartedAt(ps.getPublishStartedAt());
			    		response.setPublishCompletedAt(ps.getPublishCompletedAt());
			    		
			    		if (es != null && pes.getExecuteStartedAt().equals(es.getExecuteStartedAt())) {
			    			response.setNewExecution(false);
			    		} else {
			    			response.setNewExecution(true);
			    		}
			    	} else {
			    		
			    		response.setPublishStartedAt(ps.getPublishStartedAt());
			    		response.setPublishCompletedAt(ps.getPublishCompletedAt());
			    		
			    		response.setPublishedFromCurrentFileSystem(false);
			    	}
		        // for compatibility <--		    		
		    	} else { 
		    		response.setLegacy(true);
		    		response.setPublishStartedAt(ps.getPublishStartedAt());
		    		response.setPublishCompletedAt(ps.getPublishCompletedAt());
		        // for compatibility -->		    		
		    	}
	    	} else {
	    		response.setPublishState(DatasetState.UNPUBLISHED);
	    	}
    	}
    	response.setBinding(doc.getBinding());
    	
        return response;
    }
    
    public VocabularizerResponse vocabularizer2VocabularizerResponse(VocabularizerDocument doc) {
    	VocabularizerResponse response = new VocabularizerResponse();
    	response.setId(doc.getId().toString());
    	response.setUuid(doc.getUuid());
    	response.setOnProperty(doc.getOnProperty());
    	response.setSeparator(doc.getSeparator());
    	MappingExecuteState es = doc.checkExecuteState(fileSystemConfiguration.getId());
    	if (es != null) {    	
	    	response.setExecuteState(es.getExecuteState());
	    	response.setExecuteStartedAt(es.getExecuteStartedAt());
	    	response.setExecuteCompletedAt(es.getExecuteCompletedAt());
	    	response.setCount(es.getCount());
	    	
    	}
    	for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
	    	PublishState ps = doc.checkPublishState(vc.getId());
	    	if (ps != null) {    	
		    	response.setPublishState(ps.getPublishState());
		    	response.setPublishStartedAt(ps.getPublishStartedAt());
		    	response.setPublishCompletedAt(ps.getPublishCompletedAt());
	    	}
    	}
    	for (ElasticConfiguration ec : elasticConfigurations.values()) { // currently support only one publication site
	    	IndexState is = doc.checkIndexState(ec.getId());
	    	if (is != null) {    	
		    	response.setIndexState(is.getIndexState());
		    	response.setIndexStartedAt(is.getIndexStartedAt());
		    	response.setIndexCompletedAt(is.getIndexCompletedAt());
	    	}
    	}
    	response.setName(doc.getName());
    	
        return response;
    }
    
//    public IndexDocumentResponse index2IndexResponse(IndexDocument doc) {
//    	IndexDocumentResponse response = new IndexDocumentResponse();
//    	response.setId(doc.getId().toString());
//    	response.setUuid(doc.getUuid());
////    	response.setOnProperties(doc.getOnProperties());
//    	response.setIndexState(doc.getIndexState());
//    	response.setIndexStartedAt(doc.getIndexStartedAt());
//    	response.setIndexCompletedAt(doc.getIndexCompletedAt());
////    	response.setHost(doc.getHost());
//    	
//        return response;
//    }    
    
    public DatabaseResponse database2DatabaseResponse(Database doc, LodViewConfiguration lodview, List<String> tripleStores, List<String> indexEngines) {
    	DatabaseResponse response = new DatabaseResponse();
    	response.setId(doc.getId().toString());
    	response.setName(doc.getName());
    	response.setLabel(doc.getLabel());
    	response.setResourcePrefix(doc.getResourcePrefix());
    	
    	if (lodview != null) {
    		response.setLodview(lodview.getBaseUrl());
    	}
    	
    	response.setTripleStores(tripleStores);
    	response.setIndexEngines(indexEngines);
    	
        return response;
	}
    
    public IndexStructureResponse indexStructure2IndexStructureResponse(IndexStructure doc) {
    	IndexStructureResponse response = new IndexStructureResponse();
    	response.setId(doc.getId().toString());
    	response.setIdentifier(doc.getIdentifier());
    	response.setIndexEngine(elasticConfigurations.getById(doc.getElasticConfigurationId()).getName());
    	if (doc.getElements() != null) {
    		List<ClassIndexElementResponse> elements = new ArrayList<>();
    		for (ClassIndexElement cie : doc.getElements()) {
    			elements.add(indexStructure2IndexStructureResponse(cie));
    		}

    		response.setElements(elements);
    	}

    	response.setKeysMetadata(doc.getKeysMetadata());
//    	response.setKeyMap(doc.getKeyMap());
    	
        return response;
	}

    private ClassIndexElementResponse indexStructure2IndexStructureResponse(ClassIndexElement cie) {
    	ClassIndexElementResponse response = new ClassIndexElementResponse();
    	response.setClazz(cie.getClazz());
    	if (cie.getEmbedders() != null) {
    		List<EmbedderIndexElementResponse> embedders = new ArrayList<>();
    		for (EmbedderIndexElement eie : cie.getEmbedders()) {
    			embedders.add(indexStructure2IndexStructureResponse(eie));
    		}

    		response.setEmbedders(embedders);
    	}
    	
    	if (cie.getProperties() != null) {
    		List<PropertyIndexElementResponse> properties = new ArrayList<>();
    		for (PropertyIndexElement pie : cie.getProperties()) {
    			properties.add(indexStructure2IndexStructureResponse(pie));
    		}

    		response.setProperties(properties);
    	}
    	
        return response;
	}
    
    private EmbedderIndexElementResponse indexStructure2IndexStructureResponse(EmbedderIndexElement eie) {
    	EmbedderIndexElementResponse response = new EmbedderIndexElementResponse();
    	response.setEmbedderId(eie.getEmbedderId().toString());
    	response.setSelectors(eie.getSelectors());
    	
        return response;
	}
    
    private PropertyIndexElementResponse indexStructure2IndexStructureResponse(PropertyIndexElement pie) {
    	PropertyIndexElementResponse response = new PropertyIndexElementResponse();
    	response.setProperty(pie.getProperty());
    	response.setSelectors(pie.getSelectors());
    	if (pie.getElement() != null) {
    		response.setElement(indexStructure2IndexStructureResponse(pie.getElement()));
    	}
    	
        return response;
	}

    public AnnotationEditGroupResponse annotationEditGroup2AnnotationEditGroupResponse(TripleStoreConfiguration vc, AnnotationEditGroup aeg, List<PagedAnnotationValidation> pavs, List<FilterAnnotationValidation> favs) {

    	AnnotationEditGroupResponse aegr = new AnnotationEditGroupResponse();
    	aegr.setId(aeg.getId().toString());
    	aegr.setUuid(aeg.getUuid());
    	aegr.setDatasetUuid(aeg.getDatasetUuid());
    	aegr.setAsProperty(aeg.getAsProperty());
    	aegr.setOnProperty(PathElement.onPathElementListAsStringListInverse(aeg.getOnProperty(), null));
   		aegr.setAutoexportable(aeg.getAutoexportable() != null ? aeg.getAutoexportable() : false);


    	List<PagedAnnotationValidationResponse> pavList = new ArrayList<>();
    	
    	for (PagedAnnotationValidation pav: pavs) {
//    		PagedAnnotationValidationResponse pavr = new PagedAnnotationValidationResponse();
//    		pavr.setId(pav.getId().toString());
//    		pavr.setUuid(pav.getUuid());
//    		pavr.setName(pav.getName());
//    		pavr.setMode(pav.getMode());
//    		pavr.setAnnotatedPagesCount(pav.getAnnotatedPagesCount());
//    		pavr.setNonAnnotatedPagesCount(pav.getNonAnnotatedPagesCount());
//    		pavr.setComplete(pav.isComplete());
//    		pavr.setLifecycleState(pav.getLifecycle());
//    		if (pav.getLifecycle() == PagedAnnotationValidationState.RESUMING || pav.getLifecycle() == PagedAnnotationValidationState.RESUMING_FAILED) {
//    			pavr.setLifecycleStartedAt(pav.getResumingStartedAt());
//    		} else {
//    			pavr.setLifecycleStartedAt(pav.getLifecycleStartedAt());
//    		}
//    		pavr.setLifecycleCompletedAt(pav.getLifecycleCompletedAt());
//        	ExecuteState pess = pav.checkExecuteState(fileSystemConfiguration.getId());
//        	if (pess != null) {
//    	    	pavr.setExecuteState(pess.getExecuteState());
//    	    	pavr.setExecuteStartedAt(pess.getExecuteStartedAt());
//    	    	pavr.setExecuteCompletedAt(pess.getExecuteCompletedAt());
////    	    	pavr.setCount(ess.getCount());
//        	}
//        	for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) { // currently support only one publication site        	
//	        	PublishState ppss = pav.checkPublishState(vc.getId()); // @FIXED FROM .getDatabaseId();
//	        	if (ppss != null) {
//	    	    	pavr.setPublishState(ppss.getPublishState());
//	    	    	pavr.setPublishStartedAt(ppss.getPublishStartedAt());
//	    	    	pavr.setPublishCompletedAt(ppss.getPublishCompletedAt());
//	        	}
//        	}
//    		pavList.add(pavr);
    		pavList.add(pagedAnnotationValidation2PagedAnnotationValidationResponse(vc, pav));
    	}
    	
    	List<FilterAnnotationValidationResponse> favList = new ArrayList<>();
    	for (FilterAnnotationValidation fav: favs) {
//    		FilterAnnotationValidationResponse favr = new FilterAnnotationValidationResponse();
//    		favr.setId(fav.getId().toString());
//    		favr.setUuid(fav.getUuid());
//    		favr.setName(fav.getName());
//    		favr.setFilters(fav.getFilters());
//        	ExecuteState pess = fav.checkExecuteState(fileSystemConfiguration.getId());
//        	if (pess != null) {
//    	    	favr.setExecuteState(pess.getExecuteState());
//    	    	favr.setExecuteStartedAt(pess.getExecuteStartedAt());
//    	    	favr.setExecuteCompletedAt(pess.getExecuteCompletedAt());
//        	}
////        	for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) { // currently support only one publication site        	
//	        	PublishState ppss = fav.checkPublishState(vc.getId()); // @FIXED FROM .getDatabaseId();
//	        	if (ppss != null) {
//	    	    	favr.setPublishState(ppss.getPublishState());
//	    	    	favr.setPublishStartedAt(ppss.getPublishStartedAt());
//	    	    	favr.setPublishCompletedAt(ppss.getPublishCompletedAt());
//	        	}
////        	}
//    		favList.add(favr);
    		favList.add(filterAnnotationValidation2FilterAnnotationValidationResponse(vc, fav));
    	}
    	aegr.setPagedAnnotationValidations(pavList);
    	aegr.setFilterAnnotationValidations(favList);

        return aegr;
    }
    
    public PagedAnnotationValidationResponse pagedAnnotationValidation2PagedAnnotationValidationResponse(TripleStoreConfiguration vc, PagedAnnotationValidation pav) {

    	PagedAnnotationValidationResponse pavr = new PagedAnnotationValidationResponse();
    	pavr.setId(pav.getId().toString());
    	pavr.setUuid(pav.getUuid());
    	pavr.setName(pav.getName());
    	pavr.setMode(pav.getMode());
    	pavr.setAnnotationEditGroupId(pav.getAnnotationEditGroupId().toString());
    	
		pavr.setAnnotatedPagesCount(pav.getAnnotatedPagesCount());
		pavr.setNonAnnotatedPagesCount(pav.getNonAnnotatedPagesCount());
		pavr.setComplete(pav.isComplete());
		
    	MappingExecuteState es = pav.checkExecuteState(fileSystemConfiguration.getId());
    	if (es != null) {    	
	    	pavr.setExecuteState(es.getExecuteState());
	    	pavr.setExecuteStartedAt(es.getExecuteStartedAt());
	    	pavr.setExecuteCompletedAt(es.getExecuteCompletedAt());
	    	pavr.setExecuteMessages(es.getMessages());
	    	pavr.setExecuteCount(es.getCount());
    	}    	

    	if (vc != null) { 
    		MappingPublishState ps = pav.checkPublishState(vc.getId());
    		
	    	if (ps != null) {    	
		    	pavr.setPublishState(ps.getPublishState());
		    	MappingExecuteState pes = ps.getExecute();
		    	
		    	if (pes != null) {
		    		if (pes.getDatabaseConfigurationId().equals(fileSystemConfiguration.getId())) {
		    			pavr.setPublishedFromCurrentFileSystem(true);
			    		
		    			pavr.setPublishStartedAt(ps.getPublishStartedAt());
		    			pavr.setPublishCompletedAt(ps.getPublishCompletedAt());
			    		
			    		if (es != null && pes.getExecuteStartedAt().equals(es.getExecuteStartedAt())) {
			    			pavr.setNewExecution(false);
			    		} else {
			    			pavr.setNewExecution(true);
			    		}
			    	} else {
			    		
			    		pavr.setPublishStartedAt(ps.getPublishStartedAt());
			    		pavr.setPublishCompletedAt(ps.getPublishCompletedAt());
			    		
			    		pavr.setPublishedFromCurrentFileSystem(false);
			    	}
		        // for compatibility <--		    		
		    	} else { 
		    		pavr.setPublishStartedAt(ps.getPublishStartedAt());
		    		pavr.setPublishCompletedAt(ps.getPublishCompletedAt());
		        // for compatibility -->		    		
		    	}
	    	} else {
	    		pavr.setPublishState(DatasetState.UNPUBLISHED);
	    	} 
    	}
    	
//    	for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) { // currently support only one publication site    	
//	    	PublishState ps = pav.checkPublishState(vc.getId());
//	    	if (ps != null) {    	
//		    	pavr.setPublishState(ps.getPublishState());
//		    	pavr.setPublishStartedAt(ps.getPublishStartedAt());
//		    	pavr.setPublishCompletedAt(ps.getPublishCompletedAt());
//	    	}
//    	}  
    	
    	pavr.setLifecycleState(pav.getLifecycle());
//    	pavr.setLifecycleStartedAt(pav.getLifecycleStartedAt());
		if (pav.getLifecycle() == PagedAnnotationValidationState.RESUMING || pav.getLifecycle() == PagedAnnotationValidationState.RESUMING_FAILED) {
			pavr.setLifecycleStartedAt(pav.getResumingStartedAt());
		} else {
			pavr.setLifecycleStartedAt(pav.getLifecycleStartedAt());
		}
    	pavr.setLifecycleCompletedAt(pav.getLifecycleCompletedAt());
    	
        return pavr;
    }

    public FilterAnnotationValidationResponse filterAnnotationValidation2FilterAnnotationValidationResponse(TripleStoreConfiguration vc, FilterAnnotationValidation fav) {

		FilterAnnotationValidationResponse favr = new FilterAnnotationValidationResponse();
		favr.setId(fav.getId().toString());
		favr.setUuid(fav.getUuid());
		favr.setName(fav.getName());
		favr.setFilters(fav.getFilters());
		
//    	ExecuteState pess = fav.checkExecuteState(fileSystemConfiguration.getId());
//    	if (pess != null) {
//	    	favr.setExecuteState(pess.getExecuteState());
//	    	favr.setExecuteStartedAt(pess.getExecuteStartedAt());
//	    	favr.setExecuteCompletedAt(pess.getExecuteCompletedAt());
//    	}
		
    	MappingExecuteState es = fav.checkExecuteState(fileSystemConfiguration.getId());
    	if (es != null) {    	
	    	favr.setExecuteState(es.getExecuteState());
	    	favr.setExecuteStartedAt(es.getExecuteStartedAt());
	    	favr.setExecuteCompletedAt(es.getExecuteCompletedAt());
	    	favr.setExecuteMessages(es.getMessages());
	    	favr.setExecuteCount(es.getCount());
    	}    
    	
////    	for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) { // currently support only one publication site    	
//	    	PublishState ppss = fav.checkPublishState(vc.getId()); // @FIXED FROM .getDatabaseId();
//	    	if (ppss != null) {
//		    	favr.setPublishState(ppss.getPublishState());
//		    	favr.setPublishStartedAt(ppss.getPublishStartedAt());
//		    	favr.setPublishCompletedAt(ppss.getPublishCompletedAt());
//	    	}
////    	}
    	
    	if (vc != null) { 
    		MappingPublishState ps = fav.checkPublishState(vc.getId());
    		
	    	if (ps != null) {    	
		    	favr.setPublishState(ps.getPublishState());
		    	MappingExecuteState pes = ps.getExecute();
		    	
		    	if (pes != null) {
		    		if (pes.getDatabaseConfigurationId().equals(fileSystemConfiguration.getId())) {
		    			favr.setPublishedFromCurrentFileSystem(true);
			    		
		    			favr.setPublishStartedAt(ps.getPublishStartedAt());
		    			favr.setPublishCompletedAt(ps.getPublishCompletedAt());
			    		
			    		if (es != null && pes.getExecuteStartedAt().equals(es.getExecuteStartedAt())) {
			    			favr.setNewExecution(false);
			    		} else {
			    			favr.setNewExecution(true);
			    		}
			    	} else {
			    		
			    		favr.setPublishStartedAt(ps.getPublishStartedAt());
			    		favr.setPublishCompletedAt(ps.getPublishCompletedAt());
			    		
			    		favr.setPublishedFromCurrentFileSystem(false);
			    	}
		        // for compatibility <--		    		
		    	} else { 
		    		favr.setPublishStartedAt(ps.getPublishStartedAt());
		    		favr.setPublishCompletedAt(ps.getPublishCompletedAt());
		        // for compatibility -->		    		
		    	}
	    	} else {
	    		favr.setPublishState(DatasetState.UNPUBLISHED);
	    	} 
    	}    	
    	
        return favr;
    }
    
    public PagedAnnotationValidationPageResponse pagedAnnotationValidationPage2PagedAnnotationValidationPageResponse(PagedAnnotationValidationPage pavp) {

    	PagedAnnotationValidationPageResponse pavpr = new PagedAnnotationValidationPageResponse();
    	pavpr.setId(pavp.getId().toString());
    	pavpr.setAnnotationEditGroupId(pavp.getAnnotationEditGroupId().toString());
    	pavpr.setPagedAnnotationValidationId(pavp.getPagedAnnotationValidationId().toString());
    	pavpr.setMode(pavp.getMode());
    	pavpr.setPage(pavp.getPage());
    	pavpr.setAnnotationsCount(pavp.getAnnotationsCount());
    	pavpr.setAddedCount(pavp.getAddedCount());
    	pavpr.setValidatedCount(pavp.getValidatedCount());
    	pavpr.setUnvalidatedCount(pavp.getUnvalidatedCount());

        return pavpr;
    }


    
    public DataServiceResponse dataService2DataServiceResponse(DataService ds) {

    	DataServiceResponse dsr = new DataServiceResponse();
    	dsr.setIdentifier(ds.getIdentifier());
    	dsr.setTitle(ds.getTitle());
    	dsr.setParameters(ds.getParameters());
    	dsr.setAsProperties(ds.getAsProperties());
    	dsr.setVariants(ds.getVariants());
    	dsr.setDescription(ds.getDescription());

        return dsr;
    }
    
    public TemplateResponse template2TemplateResponse(Template tt) {

    	TemplateResponse ttr = new TemplateResponse();
    	ttr.setId(tt.getId().toString());
    	ttr.setName(tt.getName());
    	ttr.setParameters(tt.getParameters());

        return ttr;
    }
    
    public VocabularyResponse vocabulary2VocabularyResponse(Vocabulary voc) {

    	VocabularyResponse vocr = new VocabularyResponse();
    	vocr.setName(voc.getName());
    	vocr.setNamespace(voc.getNamespace());
    	vocr.setPrefix(voc.getPrefix());
    	vocr.setClasses(voc.getClasses());
    	vocr.setProperties(voc.getProperties());
        
    	return vocr;
    }

    public CampaignResponse campaign2CampaignResponse(Campaign camp) {

    	CampaignResponse campr = new CampaignResponse();
    	campr.setId(camp.getId().toString());
    	campr.setName(camp.getName());
    	campr.setType(camp.getType());
    	campr.setState(camp.getState());
        
    	return campr;
    }
    
    public TripleStoreResponse tripleStore2TripleStoreResponse(TripleStoreConfiguration ts) {

    	TripleStoreResponse response = new TripleStoreResponse();
    	
    	response.setId(ts.getId().toString());
    	response.setName(ts.getName());
    	response.setSparqlEndpoint(ts.getSparqlEndpoint());
//    	response.setFileServer(ts.getFileServer());
    	response.setType(ts.getType());
    	
        return response;
	}
    
    public UserResponse user2UserResponse(User user) {
    	return user2UserResponse(user, null);
    }
    
    public UserResponse user2UserResponse(User user, List<UserRoleType> roles) {

    	UserResponse ur = new UserResponse();
    	ur.setId(user.getId().toString());
    	ur.setName(user.getName());
    	ur.setEmail(user.getEmail());
    	ur.setRoles(roles);
        
    	return ur;
    }

}
