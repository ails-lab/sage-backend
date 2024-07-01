package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.LodViewConfiguration;
import ac.software.semantic.model.MappingDataFile;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.PagedAnnotationValidationPage;
import ac.software.semantic.model.SavedTemplate;
import ac.software.semantic.model.TemplateService;
import ac.software.semantic.model.TripleStoreConfiguration;
//import ac.software.semantic.model.VocabularizerDocument;
import ac.software.semantic.model.Vocabulary;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.EmbedderIndexElement;
import ac.software.semantic.model.index.PropertyIndexElement;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import ac.software.semantic.payload.response.ClassIndexElementResponse;
import ac.software.semantic.payload.response.DataServiceResponse;
import ac.software.semantic.payload.response.DatabaseResponse;
import ac.software.semantic.payload.response.ElasticResponse;
import ac.software.semantic.payload.response.EmbedderIndexElementResponse;
import ac.software.semantic.payload.response.IndexStructureResponse;
import ac.software.semantic.payload.response.MappingInstanceResponse;
import ac.software.semantic.payload.response.PagedAnnotationValidationPageResponse;
import ac.software.semantic.payload.response.PropertyIndexElementResponse;
import ac.software.semantic.payload.response.TemplateResponse;
import ac.software.semantic.payload.response.TripleStoreResponse;
//import ac.software.semantic.payload.response.VocabularizerResponse;
import ac.software.semantic.payload.response.VocabularyResponse;

@Component
public class ModelMapper {

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("elastic-configurations")
    private ConfigurationContainer<ElasticConfiguration> elasticConfigurations;

    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;


    public MappingInstanceResponse mappingInstance2MappingInstanceResponse(TripleStoreConfiguration vc, MappingInstance doc) {
    	MappingInstanceResponse response = new MappingInstanceResponse();
    	response.setId(doc.getId().toString());
   		List<String> sFiles = new ArrayList<>();
   		for (MappingDataFile mdf : doc.checkDataFiles(fileSystemConfiguration)) {
   			sFiles.add(mdf.getFilename());
   		}
   		if (sFiles.size() > 0) {
   			response.setDataFiles(sFiles);
   		}
   		response.setUuid(doc.getUuid());
    	response.setIdentifier(doc.getIdentifier());
    	response.setBinding(doc.getBinding());
    	response.setActive(doc.isActive());
    	
    	response.copyStates(doc, vc, fileSystemConfiguration);
    	
        return response;
    }
    
    
//    public VocabularizerResponse vocabularizer2VocabularizerResponse(VocabularizerDocument doc) {
//    	VocabularizerResponse response = new VocabularizerResponse();
//    	response.setId(doc.getId().toString());
//    	response.setUuid(doc.getUuid());
//    	response.setOnProperty(doc.getOnProperty());
//    	response.setSeparator(doc.getSeparator());
//    	response.setName(doc.getName());
//
//    	TripleStoreConfiguration vc = null;
//    	ProcessStateContainer psc = doc.getCurrentPublishState(virtuosoConfigurations.values());
//    	if (psc != null) {
//    		vc = psc.getTripleStoreConfiguration();
//    	}
//    	
//    	response.copyStates(doc, vc, fileSystemConfiguration);
//
//    	
//        return response;
//    }
    
    public DatabaseResponse database2DatabaseResponse(Database doc, LodViewConfiguration lodview, Collection<String> tripleStores, Collection<String> indexEngines) {
    	DatabaseResponse response = new DatabaseResponse();
    	response.setId(doc.getId().toString());
    	response.setName(doc.getName());
    	response.setLabel(doc.getLabel());
    	response.setResourcePrefix(doc.getResourcePrefix());
    	
    	if (lodview != null) {
    		response.setLodview(lodview.getBaseUrl());
    	}
    	
    	if (tripleStores != null && tripleStores.size() > 0) {
    		response.setTripleStores(tripleStores);
    	}
    	
    	if (indexEngines != null && indexEngines.size() > 0) {
    		response.setIndexEngines(indexEngines);
    	}
    	
        return response;
	}
    
//    public IndexStructureResponse indexStructure2IndexStructureResponse(IndexStructure doc) {
//    	IndexStructureResponse response = new IndexStructureResponse();
//    	response.setId(doc.getId().toString());
//    	response.setIdentifier(doc.getIdentifier());
////    	response.setIndexEngine(elasticConfigurations.getById(doc.getElasticConfigurationId()).getName());
//    	if (doc.getElements() != null) {
//    		List<ClassIndexElementResponse> elements = new ArrayList<>();
//    		for (ClassIndexElement cie : doc.getElements()) {
//    			elements.add(indexStructure2IndexStructureResponse(cie));
//    		}
//
//    		response.setElements(elements);
//    	}
//
//    	response.setKeysMetadata(doc.getKeysMetadata());
//    	
//        return response;
//	}
    
    public List<ClassIndexElementResponse> indexStructure2IndexStructureResponse(List<ClassIndexElement> cies) {
    	List<ClassIndexElementResponse> response = new ArrayList<>();
    	if (cies != null) {
    		for (ClassIndexElement cie : cies) {
    			response.add(indexStructure2IndexStructureResponse(cie));
    		}
    	}
    	
        return response;
	}

    public ClassIndexElementResponse indexStructure2IndexStructureResponse(ClassIndexElement cie) {
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
    	if (pie.getElements() != null) {
//    		response.setElement(indexStructure2IndexStructureResponse(pie.getElements()));
    		List<ClassIndexElementResponse> elements = new ArrayList<>();
    		for (ClassIndexElement cie : pie.getElements()) {
    			elements.add(indexStructure2IndexStructureResponse(cie));
    		}

    		response.setElements(elements);
    	}
    	
        return response;
	}

    public PagedAnnotationValidationPageResponse pagedAnnotationValidationPage2PagedAnnotationValidationPageResponse(PagedAnnotationValidationPage pavp) {

    	PagedAnnotationValidationPageResponse response = new PagedAnnotationValidationPageResponse();
    	response.setId(pavp.getId().toString());
    	response.setAnnotationEditGroupId(pavp.getAnnotationEditGroupId().toString());
    	response.setPagedAnnotationValidationId(pavp.getPagedAnnotationValidationId().toString());
    	response.setMode(pavp.getMode());
    	response.setPage(pavp.getPage());
    	response.setAnnotationsCount(pavp.getAnnotationsCount());
    	response.setAddedCount(pavp.getAddedCount());
    	response.setValidatedCount(pavp.getValidatedCount());
    	response.setUnvalidatedCount(pavp.getUnvalidatedCount());

        return response;
    }


    
    public DataServiceResponse dataService2DataServiceResponse(DataService ds) {

    	DataServiceResponse response = new DataServiceResponse();
    	response.setIdentifier(ds.getIdentifier());
    	response.setTitle(ds.getTitle());
    	response.setParameters(ds.getParameters());
    	response.setAsProperties(ds.getAsProperties());
    	response.setVariants(ds.getVariants());
    	response.setDescription(ds.getDescription());
    	response.setTags(ds.getTags());

        return response;
    }
    
    public TemplateResponse template2TemplateResponse(SavedTemplate tt) {

    	TemplateResponse response = new TemplateResponse();
    	response.setId(tt.getId().toString());
    	response.setName(tt.getName());
    	response.setParameters(tt.getParameters());

        return response;
    }
    
    public TemplateResponse template2TemplateResponse(TemplateService tt) {

    	TemplateResponse response = new TemplateResponse();
    	response.setId(tt.getId().toString());
    	response.setName(tt.getName());
    	response.setParameters(tt.getParameters());
    	response.setDescription(tt.getDescription());

        return response;
    }
    
    public VocabularyResponse vocabulary2VocabularyResponse(Vocabulary voc) {

    	VocabularyResponse response = new VocabularyResponse();
    	response.setId(voc.getId().toString());
    	response.setName(voc.getName());
    	response.setUriDescriptors(voc.getUriDescriptors());
    	response.setClasses(voc.getClasses());
    	response.setProperties(voc.getProperties());
        
    	return response;
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
    
    public ElasticResponse elastic2ElasticResponse(ElasticConfiguration es) {

    	ElasticResponse response = new ElasticResponse();
    	
    	response.setId(es.getId().toString());
    	response.setName(es.getName());
    	response.setLocation(es.getProtocol() + "://" + es.getIndexIp() + ":" + es.getIndexPort());
//    	response.setFileServer(ts.getFileServer());
    	response.setVersion(es.getVersion());
    	
        return response;
	}
    



}
