package ac.software.semantic.service;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import ac.software.semantic.model.*;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import ac.software.semantic.payload.AnnotationEditGroupResponse;
import ac.software.semantic.payload.AnnotatorDocumentResponse;
import ac.software.semantic.payload.DataServiceResponse;
import ac.software.semantic.payload.DatabaseResponse;
import ac.software.semantic.payload.DatasetResponse;
import ac.software.semantic.payload.FileResponse;
import ac.software.semantic.payload.FilterAnnotationValidationResponse;
import ac.software.semantic.payload.IndexDocumentResponse;
import ac.software.semantic.payload.MappingInstanceResponse;
import ac.software.semantic.payload.MappingResponse;
import ac.software.semantic.payload.PagedAnnotationValidationPageResponse;
import ac.software.semantic.payload.PagedAnnotationValidationResponse;
import ac.software.semantic.payload.VocabularizerResponse;

import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.security.UserPrincipal;

@Component
public class ModelMapper {

	@Autowired
	DatasetRepository datasetRepository;
	
    @Value("${mapping.uploaded-files.folder}")
    private String uploadsFolder;
    
    @Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfiguration;

    @Autowired
    @Qualifier("elastic-configuration")
    private ElasticConfiguration elasticConfiguration;

    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;

    public ModelMapper() {  }
    
    public MappingResponse mapping2MappingResponse(Collection<VirtuosoConfiguration> vcs, MappingDocument doc, UserPrincipal currentUser) {
    	MappingResponse response = new MappingResponse();
    	response.setId(doc.getId().toString());
    	response.setName(doc.getName());
    	response.setD2RML(doc.getD2RML());
    	response.setDatasetId(doc.getDatasetId().toString());
    	response.setUuid(doc.getUuid());
    	response.setType(doc.getType().toString());
    	response.setParameters(doc.getParameters());
    	response.setFileName(doc.getFileName());
    	
    	List<MappingInstanceResponse> instances = new ArrayList<>();
    	for (MappingInstance inst : doc.getInstances()) {
    		instances.add(this.mappingInstance2MappingInstanceResponse(vcs, inst));
    	}
    	
    	response.setInstances(instances);
    	
    	
//    	response.setExecuteStartedAt(doc.getExecuteStartedAt());
//    	response.setExecuteCompletedAt(doc.getExecuteCompletedAt());
//    	response.setState(doc.getState());
    	
    	ArrayList<String> files = new ArrayList<>();
    	
    	File f = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder + doc.getId().toString());
    	if (f.exists() && f.isDirectory()) {
    		for (File df : f.listFiles()) {
    			if (df.isFile()) {
    				files.add(df.getName());
    			}
    		}
    	}
    	
    	response.setFiles(files);

        return response;
    }
    
    public FileResponse file2FileResponse(Collection<VirtuosoConfiguration> vcs, FileDocument doc, UserPrincipal currentUser) {
    	FileResponse response = new FileResponse();
    	response.setId(doc.getId().toString());
    	response.setName(doc.getName());
    	response.setDatasetId(doc.getDatasetId().toString());
    	response.setUuid(doc.getUuid());
    	response.setFileName(doc.getFileName());
    	for (VirtuosoConfiguration vc : vcs) { // currently support only one publication site    	
	    	PublishState ps = doc.checkPublishState(vc.getId());
	    	if (ps != null) {
		    	response.setPublishStartedAt(ps.getPublishStartedAt());
		    	response.setPublishCompletedAt(ps.getPublishCompletedAt());
		    	response.setPublishState(ps.getPublishState());    	
	    	}
    	}
    	
    	ArrayList<String> files = new ArrayList<>();
    	
    	File f = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder + doc.getId().toString());
    	if (f.exists() && f.isDirectory()) {
    		for (File df : f.listFiles()) {
    			if (df.isFile()) {
    				files.add(df.getName());
    			}
    		}
    	}
    	
    	response.setFiles(files);

        return response;
    }

    public DatasetResponse dataset2DatasetResponse(Collection<VirtuosoConfiguration> vcs, Dataset doc) {
    	DatasetResponse response = new DatasetResponse();

    	response.setId(doc.getId().toString());
    	response.setName(doc.getName());
    	response.setType(doc.getType());
    	response.setTypeUri(doc.getTypeUri());
    	response.setUuid(doc.getUuid());
    	response.setImportType(doc.getImportType() != null ? doc.getImportType() : ImportType.CUSTOM);
    	for (VirtuosoConfiguration vc : vcs) { // currently support only one publication site
	    	PublishState ps = doc.checkPublishState(vc.getId());
	    	if (ps != null) {
	    		response.setPublishDatabase(vc.getName());
		    	response.setPublishStartedAt(ps.getPublishStartedAt());
		    	response.setPublishCompletedAt(ps.getPublishCompletedAt());
		    	response.setPublishState(ps.getPublishState());
	    	}
    	}
//    	response.setSourceUri(doc.getSourceUri());
//    	response.setTargetUri(doc.getTargetUri());
    	response.setLinks(doc.getLinks());
    	IndexState is = doc.checkIndexState(elasticConfiguration.getId());
    	if (is != null) {
	    	response.setIndexStartedAt(is.getIndexStartedAt());
	    	response.setIndexCompletedAt(is.getIndexCompletedAt());
	    	response.setIndexState(is.getIndexState());
    	}
	    		
    	for (ObjectId did : doc.getDatasets()) {
    		Optional<Dataset> op = datasetRepository.findById(did);
    		if (op.isPresent()) {
    			Dataset ds = op.get();
    			DatasetResponse mr = new DatasetResponse();
    			mr.setId(did.toString());
    			mr.setName(ds.getName());
    			mr.setType(ds.getType());
    			mr.setTypeUri(ds.getTypeUri());
    			mr.setUuid(ds.getUuid());
    			for (VirtuosoConfiguration vc : vcs) { // currently support only one publication site
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
    	    	IndexState iss = ds.checkIndexState(elasticConfiguration.getId());
    	    	if (iss != null) {
	    	    	mr.setIndexStartedAt(iss.getIndexStartedAt());
	    	    	mr.setIndexCompletedAt(iss.getIndexCompletedAt());
	    	    	mr.setIndexState(iss.getIndexState());
    	    	}    	    	
    			response.addDataset(mr);
    		}
    		
    	}

        return response;
    }
    
    public AnnotatorDocumentResponse annotator2AnnotatorResponse(Collection<VirtuosoConfiguration> vcs, AnnotatorDocument doc, AnnotationEditGroup aeg) {
    	AnnotatorDocumentResponse response = new AnnotatorDocumentResponse();
    	response.setId(doc.getId().toString());
    	response.setUuid(doc.getUuid());
    	response.setOnProperty(doc.getOnProperty());
    	response.setAsProperty(doc.getAsProperty());
    	response.setAnnotator(doc.getAnnotator());
    	response.setThesaurus(doc.getThesaurus());
    	response.setParameters(doc.getParameters());
    	response.setPreprocess(doc.getPreprocess());
    	response.setVariant(doc.getVariant());
    	ExecuteState es = doc.checkExecuteState(fileSystemConfiguration.getId());
    	if (es != null) {    	
	    	response.setExecuteState(es.getExecuteState());
	    	response.setExecuteStartedAt(es.getExecuteStartedAt());
	    	response.setExecuteCompletedAt(es.getExecuteCompletedAt());
	    	response.setCount(es.getCount());
    	}    	
    	for (VirtuosoConfiguration vc : vcs) { // currently support only one publication site    	
	    	PublishState ps = doc.checkPublishState(vc.getId());
	    	if (ps != null) {    	
		    	response.setPublishState(ps.getPublishState());
		    	response.setPublishStartedAt(ps.getPublishStartedAt());
		    	response.setPublishCompletedAt(ps.getPublishCompletedAt());
	    	}
    	}    	
    	AnnotationEditGroupResponse aegr = new AnnotationEditGroupResponse();
    	aegr.setId(aeg.getId().toString());
    	aegr.setUuid(aeg.getUuid());
    	aegr.setDatasetUuid(aeg.getDatasetUuid());
    	aegr.setAsProperty(aeg.getAsProperty());
    	aegr.setOnProperty(aeg.getOnProperty());
//    	ExecuteState ess = aeg.checkExecuteState(fileSystemConfiguration.getId());
//    	if (ess != null) {    	
//	    	aegr.setExecuteState(ess.getExecuteState());
//	    	aegr.setExecuteStartedAt(ess.getExecuteStartedAt());
//	    	aegr.setExecuteCompletedAt(ess.getExecuteCompletedAt());
//	    	aegr.setCount(ess.getCount());
//    	}    	
//    	PublishState pss = aeg.checkPublishState(virtuosoConfiguration.getDatabaseId());
//    	if (pss != null) {
//	    	aegr.setPublishState(pss.getPublishState());
//	    	aegr.setPublishStartedAt(pss.getPublishStartedAt());
//	    	aegr.setPublishCompletedAt(pss.getPublishCompletedAt());
//    	}
    	
    	response.setEditGroup(aegr);
    	
        return response;
    }
    
    public MappingInstanceResponse mappingInstance2MappingInstanceResponse(Collection<VirtuosoConfiguration> vcs, MappingInstance doc) {
    	MappingInstanceResponse response = new MappingInstanceResponse();
    	response.setId(doc.getId().toString());
    	ExecuteState es = doc.checkExecuteState(fileSystemConfiguration.getId());
    	if (es != null) {    	
	    	response.setExecuteState(es.getExecuteState());
	    	response.setExecuteStartedAt(es.getExecuteStartedAt());
	    	response.setExecuteCompletedAt(es.getExecuteCompletedAt());
	    	response.setCount(es.getCount());
    	}    	
    	for (VirtuosoConfiguration vc : vcs) { // currently support only one publication site
	    	PublishState ps = doc.checkPublishState(vc.getId());
	    	if (ps != null) {    	
		    	response.setPublishState(ps.getPublishState());
		    	response.setPublishStartedAt(ps.getPublishStartedAt());
		    	response.setPublishCompletedAt(ps.getPublishCompletedAt());
	    	}
    	}
    	
    	response.setBinding(doc.getBinding());
    	
        return response;
    }
    
    public VocabularizerResponse vocabularizer2VocabularizerResponse(Collection<VirtuosoConfiguration> vcs, VocabularizerDocument doc) {
    	VocabularizerResponse response = new VocabularizerResponse();
    	response.setId(doc.getId().toString());
    	response.setUuid(doc.getUuid());
    	response.setOnProperty(doc.getOnProperty());
    	response.setSeparator(doc.getSeparator());
    	ExecuteState es = doc.checkExecuteState(fileSystemConfiguration.getId());
    	if (es != null) {    	
	    	response.setExecuteState(es.getExecuteState());
	    	response.setExecuteStartedAt(es.getExecuteStartedAt());
	    	response.setExecuteCompletedAt(es.getExecuteCompletedAt());
	    	response.setCount(es.getCount());
    	}
    	for (VirtuosoConfiguration vc : vcs) {
	    	PublishState ps = doc.checkPublishState(vc.getId());
	    	if (ps != null) {    	
		    	response.setPublishState(ps.getPublishState());
		    	response.setPublishStartedAt(ps.getPublishStartedAt());
		    	response.setPublishCompletedAt(ps.getPublishCompletedAt());
	    	}
    	}
    	IndexState is = doc.checkIndexState(elasticConfiguration.getId());
    	if (is != null) {    	
	    	response.setIndexState(is.getIndexState());
	    	response.setIndexStartedAt(is.getIndexStartedAt());
	    	response.setIndexCompletedAt(is.getIndexCompletedAt());
    	}    	
    	response.setName(doc.getName());
    	
        return response;
    }
    
    public IndexDocumentResponse index2IndexResponse(IndexDocument doc) {
    	IndexDocumentResponse response = new IndexDocumentResponse();
    	response.setId(doc.getId().toString());
    	response.setUuid(doc.getUuid());
    	response.setOnProperties(doc.getOnProperties());
    	response.setIndexState(doc.getIndexState());
    	response.setIndexStartedAt(doc.getIndexStartedAt());
    	response.setIndexCompletedAt(doc.getIndexCompletedAt());
//    	response.setHost(doc.getHost());
    	
        return response;
    }    
    
    public DatabaseResponse database2DatabaseResponse(Database doc) {
    	DatabaseResponse response = new DatabaseResponse();
    	response.setId(doc.getId().toString());
    	response.setName(doc.getName());
    	response.setLabel(doc.getLabel());
    	
        return response;
	}

    public AnnotationEditGroupResponse annotationEditGroup2AnnotationEditGroupResponse(Collection<VirtuosoConfiguration> vcs, AnnotationEditGroup aeg, List<PagedAnnotationValidation> pavs, List<FilterAnnotationValidation> favs) {

    	AnnotationEditGroupResponse aegr = new AnnotationEditGroupResponse();
    	aegr.setId(aeg.getId().toString());
    	aegr.setUuid(aeg.getUuid());
    	aegr.setDatasetUuid(aeg.getDatasetUuid());
    	aegr.setAsProperty(aeg.getAsProperty());
    	aegr.setOnProperty(aeg.getOnProperty());
//    	ExecuteState ess = aeg.checkExecuteState(fileSystemConfiguration.getId());
//    	if (ess != null) {
////	    	aegr.setExecuteState(ess.getExecuteState());
//	    	aegr.setExecuteStartedAt(ess.getExecuteStartedAt());
//	    	aegr.setExecuteCompletedAt(ess.getExecuteCompletedAt());
//	    	aegr.setCount(ess.getCount());
//    	}
//    	PublishState pss = aeg.checkPublishState(virtuosoConfiguration.getDatabaseId());
//    	if (pss != null) {
////	    	aegr.setPublishState(pss.getPublishState());
//	    	aegr.setPublishStartedAt(pss.getPublishStartedAt());
//	    	aegr.setPublishCompletedAt(pss.getPublishCompletedAt());
//    	}

    	List<PagedAnnotationValidationResponse> pavList = new ArrayList<>();
    	for (PagedAnnotationValidation pav: pavs) {
    		PagedAnnotationValidationResponse pavr = new PagedAnnotationValidationResponse();
    		pavr.setId(pav.getId().toString());
    		pavr.setUuid(pav.getUuid());
    		System.out.println(pav.getUuid());
    		pavr.setName(pav.getName());
    		pavr.setAnnotatedPagesCount(pav.getAnnotatedPagesCount());
    		pavr.setNonAnnotatedPagesCount(pav.getNonAnnotatedPagesCount());
    		pavr.setComplete(pav.isComplete());
        	ExecuteState pess = pav.checkExecuteState(fileSystemConfiguration.getId());
        	if (pess != null) {
    	    	pavr.setExecuteState(pess.getExecuteState());
    	    	pavr.setExecuteStartedAt(pess.getExecuteStartedAt());
    	    	pavr.setExecuteCompletedAt(pess.getExecuteCompletedAt());
//    	    	pavr.setCount(ess.getCount());
        	}
        	for (VirtuosoConfiguration vc : vcs) { // currently support only one publication site        	
	        	PublishState ppss = pav.checkPublishState(vc.getDatabaseId());
	        	if (ppss != null) {
	    	    	pavr.setPublishState(ppss.getPublishState());
	    	    	pavr.setPublishStartedAt(ppss.getPublishStartedAt());
	    	    	pavr.setPublishCompletedAt(ppss.getPublishCompletedAt());
	        	}
        	}
    		pavList.add(pavr);
    	}
    	
    	List<FilterAnnotationValidationResponse> favList = new ArrayList<>();
    	for (FilterAnnotationValidation fav: favs) {
    		FilterAnnotationValidationResponse favr = new FilterAnnotationValidationResponse();
    		favr.setId(fav.getId().toString());
    		favr.setUuid(fav.getUuid());
    		favr.setName(fav.getName());
    		favr.setFilters(fav.getFilters());
        	ExecuteState pess = fav.checkExecuteState(fileSystemConfiguration.getId());
        	if (pess != null) {
    	    	favr.setExecuteState(pess.getExecuteState());
    	    	favr.setExecuteStartedAt(pess.getExecuteStartedAt());
    	    	favr.setExecuteCompletedAt(pess.getExecuteCompletedAt());
        	}
        	for (VirtuosoConfiguration vc : vcs) { // currently support only one publication site        	
	        	PublishState ppss = fav.checkPublishState(vc.getDatabaseId());
	        	if (ppss != null) {
	    	    	favr.setPublishState(ppss.getPublishState());
	    	    	favr.setPublishStartedAt(ppss.getPublishStartedAt());
	    	    	favr.setPublishCompletedAt(ppss.getPublishCompletedAt());
	        	}
        	}
    		favList.add(favr);
    	}
    	aegr.setPagedAnnotationValidations(pavList);
    	aegr.setFilterAnnotationValidations(favList);

        return aegr;
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

    public FilterAnnotationValidationResponse filterAnnotationValidation2FilterAnnotationValidationResponse(Collection<VirtuosoConfiguration> vcs, FilterAnnotationValidation fav) {

		FilterAnnotationValidationResponse favr = new FilterAnnotationValidationResponse();
		favr.setId(fav.getId().toString());
		favr.setUuid(fav.getUuid());
		favr.setName(fav.getName());
		favr.setFilters(fav.getFilters());
    	ExecuteState pess = fav.checkExecuteState(fileSystemConfiguration.getId());
    	if (pess != null) {
	    	favr.setExecuteState(pess.getExecuteState());
	    	favr.setExecuteStartedAt(pess.getExecuteStartedAt());
	    	favr.setExecuteCompletedAt(pess.getExecuteCompletedAt());
    	}
    	for (VirtuosoConfiguration vc : vcs) { // currently support only one publication site    	
	    	PublishState ppss = fav.checkPublishState(vc.getDatabaseId());
	    	if (ppss != null) {
		    	favr.setPublishState(ppss.getPublishState());
		    	favr.setPublishStartedAt(ppss.getPublishStartedAt());
		    	favr.setPublishCompletedAt(ppss.getPublishCompletedAt());
	    	}
    	}
    	
        return favr;
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

}
