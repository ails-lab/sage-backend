package ac.software.semantic.service;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.Optional;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.PublicTask;
import ac.software.semantic.model.constants.state.TaskState;
import ac.software.semantic.model.constants.type.SerializationType;
import ac.software.semantic.repository.core.AnnotationEditGroupRepository;
import ac.software.semantic.repository.core.PublicTaskRepository;

@Service
public class ContentService {

	Logger logger = LoggerFactory.getLogger(ContentService.class);
	
    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;

	@Autowired
	private PublicTaskRepository publicTaskRepository;
	
	@Autowired
	private AnnotationEditGroupRepository aegRepository;
	
	@Autowired
	private AnnotationEditGroupService aegService;
	
	@Async("annotationsExportExecutor")
	public ListenableFuture<Date> execute(String uuid, String serialization, boolean onlyReviewed, boolean onlyNonRejected, boolean onlyFresh, boolean created, boolean creator, boolean score,
			boolean scope, 
			boolean selector, String output, Dataset dataset) throws Exception {

		logger.info("Start exporting annotations for dataset " + dataset.getIdentifier());
		
		try {
			
			String file = fileSystemConfiguration.getPublicFolder() + File.separatorChar + uuid + "." + output.toLowerCase();
		
	    	if (output.equalsIgnoreCase("zip")) {
	    		  
				try (FileOutputStream outputStream = new FileOutputStream(file); 
						ZipOutputStream zos = new ZipOutputStream(outputStream)) {
					for (AnnotationEditGroup aeg : aegRepository.findByDatasetUuidAndAutoexportable(dataset.getUuid(), true)) {
						logger.info("Exporting annotations for dataset " + dataset.getIdentifier() + "/" + aeg.getId() + " started.");
		    			try {
							aegService.exportAnnotations(aeg.getId(), SerializationType.get(serialization), onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, zos);
						} catch (Exception ex) {
							logger.info("Exporting annotations for dataset " + dataset.getIdentifier() + "/" + aeg.getId() + " failed.");
							throw ex;
						}
		    			logger.info("Exporting annotations for dataset " + dataset.getIdentifier() + "/" + aeg.getId() + " completed.");
			    	}
					zos.finish();
	    		};
	    			    	
	    	
	    	} else if (output.equalsIgnoreCase("tgz"))	{
	    		
				try (FileOutputStream outputStream = new FileOutputStream(file); 
						BufferedOutputStream buffOut = new BufferedOutputStream(outputStream);
						GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(buffOut);
						TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {
	
					for (AnnotationEditGroup aeg : aegRepository.findByDatasetUuidAndAutoexportable(dataset.getUuid(), true)) {
						logger.info("Exporting annotations for dataset " + dataset.getIdentifier() + "/" + aeg.getId() + " started.");
	
						try {
							aegService.exportAnnotations(aeg.getId(), SerializationType.get(serialization), onlyReviewed, onlyNonRejected, onlyFresh, created, creator, score, scope, selector, tOut);
						} catch (Exception ex) {
							logger.info("Exporting annotations for dataset " + dataset.getIdentifier() + "/" + aeg.getId() + " failed.");
							throw ex;
						}
						
						logger.info("Exporting annotations for dataset " + dataset.getIdentifier() + "/" + aeg.getId() + " completed.");
					}
				}
	    	}
	    	
	    	Date date = new Date();
			synchronized (uuid.intern()) {
				Optional<PublicTask> ptOpt = publicTaskRepository.findByUuid(uuid);
				if (!ptOpt.isPresent()) {
					throw new Exception("Public task not found");
				}
				
				PublicTask pt = ptOpt.get();
				pt.setState(TaskState.COMPLETED);
				pt.setCompletedAt(date);
				
				publicTaskRepository.save(pt);
			}

			return new AsyncResult<>(date);
			
    	} catch (Exception ex) {
    		ex.printStackTrace();
    		
	    	Date date = new Date();
			synchronized (uuid.intern()) {
				Optional<PublicTask> ptOpt = publicTaskRepository.findByUuid(uuid);
				if (!ptOpt.isPresent()) {
					throw new Exception("Public task not found");
				}
				
				PublicTask pt = ptOpt.get();
				pt.setState(TaskState.FAILED);
				pt.setCompletedAt(date);
				
				publicTaskRepository.save(pt);
			}
			
			return new AsyncResult<>(date);
    	}
	}
	
	public void failUnfinishedTasks() {
		for (PublicTask pt : publicTaskRepository.findByStateAndFileSystemConfigurationId(TaskState.STARTED, fileSystemConfiguration.getId())) {
			pt.setState(TaskState.FAILED);
			pt.setCompletedAt(new Date());
			
			publicTaskRepository.save(pt);
			
			String output = (String)pt.getParameters().get("output");
			
    		if (output != null) {
    			String file = fileSystemConfiguration.getPublicFolder() + File.separatorChar + pt.getUuid() + "." + output.toLowerCase();
    			File f = new File(file);
    			if (f.exists()) {
    				f.delete();
    			}
    		}
		}
		
	}
}
