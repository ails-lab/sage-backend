package ac.software.semantic.service;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import ac.software.semantic.model.AnnotationValidation;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.ClustererDocument;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DistributionDocument;
import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.type.FileType;
import ac.software.semantic.model.constants.type.SerializationType;
import ac.software.semantic.model.state.CreateState;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.FileExecuteState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.payload.response.MappingInstanceResponse;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.AnnotatorService.AnnotatorContainer;
import ac.software.semantic.service.ClustererService.ClustererContainer;
import ac.software.semantic.service.EmbedderService.EmbedderContainer;
import ac.software.semantic.service.MappingService.MappingContainer;
import ac.software.semantic.service.container.AnnotationValidationContainer;
import ac.software.semantic.service.container.DataServiceContainer;
import ac.software.semantic.service.container.ExecutableContainer;
import ac.software.semantic.service.container.SideExecutableContainer;
import edu.ntua.isci.ac.d2rml.output.FileSystemRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.output.FileSystemPlainTextOutputHandler;

@Service
public class FolderService {

	Logger logger = LoggerFactory.getLogger(FolderService.class);
	
	@Autowired
	@Qualifier("filesystem-configuration")
	private FileSystemConfiguration fileSystemConfiguration;

    @Value("${dataset.distribution.folder}")
    private String distributionsFolder;

    @Value("${mapping.execution.folder}")
    private String mappingsFolder;
    
    @Value("${annotation.execution.folder}")
    private String annotationsFolder;

    @Value("${embedding.execution.folder}")
    private String embeddingsFolder;

    @Value("${clustering.execution.folder}")
    private String clusterersFolder;

    @Value("${vocabularizer.execution.folder}")
    private String vocabularizerFolder;

    @Value("${mapping.uploaded-files.folder}")
    private String uploadsFolder;    

    @Value("${annotation.manual.folder}")
    private String manualFolder;
    
	@Value("${d2rml.extract.temp-folder:null}")
	private String extractTempFolder;

    private String getDatasetDistributionFolder(UserPrincipal currentUser) {
    	if (distributionsFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + distributionsFolder.substring(0, distributionsFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + distributionsFolder;
    	}
    }

    private String getMappingsFolder(UserPrincipal currentUser) {
    	if (mappingsFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder.substring(0, mappingsFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder;
    	}
    }

    private String getUploadsFolder(UserPrincipal currentUser) {
    	if (uploadsFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder.substring(0, uploadsFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder;
    	}    	
    }
    
    private String getAnnotationsFolder(UserPrincipal currentUser) {
    	if (annotationsFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder.substring(0, annotationsFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder;
    	}    	
    }

    private String getClusterersFolder(UserPrincipal currentUser) {
    	if (clusterersFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + clusterersFolder.substring(0, clusterersFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + clusterersFolder;
    	}    	
    }

    
    private String getEmbeddingsFolder(UserPrincipal currentUser) {
    	if (embeddingsFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + embeddingsFolder.substring(0, embeddingsFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + embeddingsFolder;
    	}    	
    }
    
//    public String getFolder(UserPrincipal currentUser, String rootFolder, String subFolder) {
//    	if (rootFolder.endsWith("/")) {
//    		return fileSystemConfiguration.getUserDataFolder(currentUser) + rootFolder + subFolder;
//    	} else {
//    		return fileSystemConfiguration.getUserDataFolder(currentUser) + rootFolder + "/" + subFolder;
//    	}    	
//    }
    
    public String getVocabularizerFolder(UserPrincipal currentUser) {
    	if (vocabularizerFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + vocabularizerFolder.substring(0, vocabularizerFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + vocabularizerFolder;
    	}    	
    }
    
    public String getManualFolder(UserPrincipal currentUser) {
    	if (manualFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + manualFolder.substring(0, manualFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + manualFolder;
    	}    	
    }    
    
    public String getExtractTempFolder(UserPrincipal currentUser) {
    	if (mappingsFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + extractTempFolder.substring(0, extractTempFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + extractTempFolder;
    	}
    }

    public void checkCreateExtractTempFolder(UserPrincipal currentUser) {
    	File df = new File(getExtractTempFolder(currentUser));
    	if (!df.exists()) {
    		logger.info("Created tmp folder " + getExtractTempFolder(currentUser));
    		df.mkdir();
    	}
    }

    private String getDatasetDistributionFolder(UserPrincipal currentUser, Dataset dataset) {
		return getDatasetDistributionFolder(currentUser) + "/" + dataset.getUuid();
	}

    private String getMappingsExecutionDatasetFolder(UserPrincipal currentUser, Dataset dataset) {
		return getMappingsFolder(currentUser) + "/" + dataset.getUuid();
	}
    
    private String getAnnotationsExecutionDatasetFolder(UserPrincipal currentUser, Dataset dataset) {
		return getAnnotationsFolder(currentUser) + "/" + dataset.getUuid();
	}

    private String getClusteringsExecutionDatasetFolder(UserPrincipal currentUser, Dataset dataset) {
		return getClusterersFolder(currentUser) + "/" + dataset.getUuid();
	}
    
    private String getEmbeddingsExecutionDatasetFolder(UserPrincipal currentUser, Dataset dataset) {
		return getEmbeddingsFolder(currentUser) + "/" + dataset.getUuid();
	}
    
	//for compatibility
    public String buildNoTimeStampFileName(MappingDocument doc, MappingInstanceResponse mi, int shard) {
		return doc.getUuid() + (!mi.hasBinding() ? "" : "_" + mi.getId().toString()) + (shard == 0 ? "" : "_#" + shard);
	}

    public String buildNoTimeStampFileName(SpecificationDocument doc, int shard) {
		return doc.getUuid() + (shard == 0 ? "" : "_#" + shard);
    }
    
    public String buildNoTimeStampFileName(MappingDocument doc, MappingInstance mi, int shard) {
		return doc.getUuid() + (!mi.hasBinding() ? "" : "_" + mi.getId().toString()) + (shard == 0 ? "" : "_#" + shard);
	}

    public String buildFileName(MappingDocument doc, MappingInstance mi, ExecuteState es, int shard) {
		return es.getExecuteStartStamp() + "_" + doc.getUuid() + (!mi.hasBinding() ? "" : "_" + mi.getId().toString()) + (shard == 0 ? "" : "_#" + shard);
	}

    public String buildFileName(SpecificationDocument doc, ExecuteState es, int shard) {
		return es.getExecuteStartStamp() + "_" + doc.getUuid() + (shard == 0 ? "" : "_#" + shard);
	}

    public String buildFileName(MappingDocument doc, MappingInstanceResponse mi, ExecuteState es, int shard) {
		return es.getExecuteStartStamp() + "_" + doc.getUuid() + (!mi.hasBinding() ? "" : "_" + mi.getId().toString()) + (shard == 0 ? "" : "_#" + shard);
	}
    
    public String buildFileName(Dataset dataset, PublishState<?> cds) {
		return cds.getPublishStartStamp() + "_" + dataset.getUuid();
	}
    
    public String buildFileName(DistributionDocument doc, CreateState cds) {
		return cds.getCreateStartStamp() + "_" + doc.getUuid();
	}

    
    public String getParent(File f) {
    	return f.getParent().replaceAll("\\\\", "/");
    }

    public String getName(File f) {
    	return f.getName();
    }

    // EXECUTION FILES

    public boolean deleteDatasetsDistributionFolderIfEmpty(UserPrincipal currentUser, Dataset dataset) {
		File ff = new File(getDatasetDistributionFolder(currentUser, dataset));
		if (ff.exists() && ff.isDirectory() && ff.list().length == 0) {
			boolean ok = ff.delete();
			if (ok) {
				logger.info("Deleted folder " + ff.getAbsolutePath());
				return true;
			}
		}    	
		return false;
    }
    
    public boolean deleteMappingsExecutionDatasetFolderIfEmpty(UserPrincipal currentUser, Dataset dataset) {
		File ff = new File(getMappingsExecutionDatasetFolder(currentUser, dataset));
		if (ff.exists() && ff.isDirectory() && ff.list().length == 0) {
			boolean ok = ff.delete();
			if (ok) {
				logger.info("Deleted folder " + ff.getAbsolutePath());
				return true;
			}
		}    	
		return false;
    }
    
    public String getContainingFolder(ExecutableContainer<?,?,?,Dataset> oc) {
    	if (oc instanceof AnnotationValidationContainer) {
    		return getAnnotationsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject());
    	} else if (oc instanceof AnnotatorContainer) {
        	return getAnnotationsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject());
    	} else if (oc instanceof EmbedderContainer) {
        	return getEmbeddingsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject());
    	} else if (oc instanceof ClustererContainer) {
        	return getClusteringsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject());
    	}
    	
    	return null;
    }

    public boolean deleteContainingFolderIfEmpty(ExecutableContainer<?,?,?,Dataset> oc) {
		File ff = new File(getContainingFolder(oc));
		if (ff.exists() && ff.isDirectory() && ff.list().length == 0) {
			boolean ok = ff.delete();
			if (ok) {
				logger.info("Deleted folder " + ff.getAbsolutePath());
				return true;
			}
		}    	
		return false;
    }

    
    private String createDatasetDistributionFolder(UserPrincipal currentUser, Dataset dataset) {
		String df = getDatasetDistributionFolder(currentUser, dataset);
		if (!new File(df).exists()) {
			new File(df).mkdir();
		}
		
		return df;
    }

    public File createDatasetDistributionFile(UserPrincipal currentUser, Dataset dataset, DistributionDocument doc, CreateState cds, SerializationType st) {
    	return new File(createDatasetDistributionFolder(currentUser, dataset), buildFileName(doc, cds) + "." + st.toString().toLowerCase());
    }

    public File createDatasetDistributionZipFile(UserPrincipal currentUser, Dataset dataset, DistributionDocument doc, CreateState cds, SerializationType st) {
    	return new File(createDatasetDistributionFolder(currentUser, dataset), buildFileName(doc, cds) + "_" + st.toString().toLowerCase() + ".zip");
    }

    public File createMappingsExecutionZipFile(UserPrincipal currentUser, Dataset dataset, MappingDocument doc, MappingInstance mi, ExecuteState es) {
    	return new File(getMappingsExecutionDatasetFolder(currentUser, dataset), buildFileName(doc, mi, es, 0)  + ".zip");
    }
    

    public FileSystemRDFOutputHandler createMappingExecutionRDFOutputHandler(MappingContainer mc, int shardSize) {
    	String df = getMappingsExecutionDatasetFolder(mc.getObjectOwner(), mc.getEnclosingObject());
		if (!new File(df).exists()) {
			new File(df).mkdir();
		}
    	return new FileSystemRDFOutputHandler(df, buildFileName(mc.getObject(), mc.getMappingInstance(), mc.getExecuteState(), 0), shardSize);
    }
    
    public FileSystemRDFOutputHandler createExecutionRDFOutputHandler(ExecutableContainer<?,?,?,Dataset> oc, int shardSize) {
    	String df = null;
    	if (oc instanceof AnnotatorContainer) {
    		df = getAnnotationsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject());
    	} else if (oc instanceof EmbedderContainer) {
        	df = getEmbeddingsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject());
    	} else if (oc instanceof ClustererContainer) {
        	df = getClusteringsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject());
    	} else if (oc instanceof AnnotationValidationContainer) {
    		df = getAnnotationsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject());
    	}
    	
    	if (df != null) {
			if (!new File(df).exists()) {
				new File(df).mkdir();
			}
	    	return new FileSystemRDFOutputHandler(df, buildFileName(oc.getObject(), oc.getExecuteState(), 0), shardSize);
    	} else {
    		return null;
    	}
    }

    public FileSystemPlainTextOutputHandler createMappingExecutionPlainTextOutputHandler(MappingContainer mc) {
    	String df = getMappingsExecutionDatasetFolder(mc.getObjectOwner(), mc.getEnclosingObject());
		if (!new File(df).exists()) {
			new File(df).mkdir();
		}
    	return new FileSystemPlainTextOutputHandler(df, buildFileName(mc.getObject(), mc.getMappingInstance(), mc.getExecuteState(), 0));
    }

    public File getDatasetDistributionFile(UserPrincipal currentUser, Dataset dataset, DistributionDocument doc, CreateState cds, SerializationType st) {
   		String datasetFolder = getDatasetDistributionFolder(currentUser, dataset);
   		String fileName = buildFileName(doc, cds) + "." + st.toString().toLowerCase();
    		
   		File file = new File(datasetFolder + "/" + fileName); 
   		if (file.exists()) {
   			return file;
   		}

   		return null;
    }
    
    public File getDatasetDistributionZipFile(UserPrincipal currentUser, Dataset dataset, DistributionDocument doc, CreateState cds, SerializationType st) {
   		String datasetFolder = getDatasetDistributionFolder(currentUser, dataset);
   		String fileName = buildFileName(doc, cds) + "_" + st.toString().toLowerCase() + ".zip";
    		
   		File file = new File(datasetFolder + "/" + fileName); 
   		if (file.exists()) {
   			return file;
   		}

   		return null;
    }
    
    public File createExecutionCatalogFile(SideExecutableContainer<?,?,?,Dataset> oc, ExecuteState es) {
   		return new File(getAnnotationsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject()), buildFileName(oc.getObject(), es, 0)  + "_catalog.trig");
    }
    
    public File createExecutionFile(ExecutableContainer<?,?,?,Dataset> oc, ExecuteState es, FileType fileType) {
    	if (oc instanceof AnnotationValidationContainer) {
       		return new File(getAnnotationsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject()), buildFileName(((AnnotationValidationContainer<?,?,Dataset>)oc).getAnnotationValidation(), es, 0)  + "." + fileType);
    	} else if (oc instanceof AnnotatorContainer) {
    		return new File(getAnnotationsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject()), buildFileName(((AnnotatorContainer)oc).getObject(), es, 0)  + "." + fileType);
    	} else if (oc instanceof EmbedderContainer) {
    		return new File(getEmbeddingsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject()), buildFileName(((EmbedderContainer)oc).getObject(), es, 0)  + "." + fileType);
    	} else if (oc instanceof ClustererContainer) {
    		return new File(getClusteringsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject()), buildFileName(((ClustererContainer)oc).getObject(), es, 0)  + "." + fileType);
    	} else if (oc instanceof MappingContainer) {
    		return new File(getMappingsExecutionDatasetFolder(oc.getObjectOwner(), oc.getEnclosingObject()), buildFileName(((MappingContainer)oc).getObject(), ((MappingContainer)oc).getMappingInstance(), es, 0)  + "." + fileType);
    	}
    	
    	return null;
    }

    
    public <D extends SpecificationDocument, I extends EnclosingDocument> 
    	File getExecutionTrigFile(ExecutableContainer<D,?,?,I> oc, ExecuteState es, int shard) {
    	if (oc instanceof AnnotationValidationContainer) {
    		return getAnnotationValidationExecutionFile(oc.getObjectOwner(), (Dataset)oc.getEnclosingObject(), ((AnnotationValidationContainer<?,?,Dataset>)oc).getAnnotationValidation(), es, shard, ".trig");
    	} else if (oc instanceof AnnotatorContainer) {
    		return getAnnotatorExecutionFile(oc.getObjectOwner(), (Dataset)oc.getEnclosingObject(), ((AnnotatorContainer)oc).getObject(), es, shard, ".trig");
    	} else if (oc instanceof EmbedderContainer) {
    		return getEmbedderExecutionFile(oc.getObjectOwner(), (Dataset)oc.getEnclosingObject(), ((EmbedderContainer)oc).getObject(), es, shard, ".trig");
    	} else if (oc instanceof ClustererContainer) {
    		return getClustererExecutionFile(oc.getObjectOwner(), (Dataset)oc.getEnclosingObject(), ((ClustererContainer)oc).getObject(), es, shard, ".trig");
    	} else if (oc instanceof MappingContainer) {
    		return getMappingExecutionFile(oc.getObjectOwner(), (Dataset)oc.getEnclosingObject(), ((MappingContainer)oc).getObject(), ((MappingContainer)oc).getMappingInstance(), es, shard, ".trig");
    	}
    	
    	return null;
   	}    
    
    public <D extends SpecificationDocument, I extends EnclosingDocument> 
    	File getExecutionCatalogFile(ExecutableContainer<D,?,?,I> oc, ExecuteState es) {
    	
    	if (oc instanceof AnnotationValidationContainer) {
    		return getAnnotationValidationExecutionFile(oc.getObjectOwner(), (Dataset)oc.getEnclosingObject(), ((AnnotationValidationContainer<?,?,Dataset>)oc).getAnnotationValidation(), es, 0, "_catalog.trig");
    	} else if (oc instanceof AnnotatorContainer) {
    		return getAnnotatorExecutionFile(oc.getObjectOwner(), (Dataset)oc.getEnclosingObject(), ((AnnotatorContainer)oc).getObject(), es, 0, "_catalog.trig");
    	} else if (oc instanceof EmbedderContainer) {
    		return getEmbedderExecutionFile(oc.getObjectOwner(), (Dataset)oc.getEnclosingObject(), ((EmbedderContainer)oc).getObject(), es, 0, "_catalog.trig");
    	}
    	
    	return null;
   	}    
    public File getExecutionFile(ExecutableContainer<?,?,?,Dataset> oc, ExecuteState es, FileType fileType) {
    	if (oc instanceof AnnotationValidationContainer) {
    		return getAnnotationValidationExecutionFile(oc.getObjectOwner(), oc.getEnclosingObject(), ((AnnotationValidationContainer<?,?,Dataset>)oc).getAnnotationValidation(), es, 0, "." + fileType);
    	} else if (oc instanceof AnnotatorContainer) {
    		return getAnnotatorExecutionFile(oc.getObjectOwner(), oc.getEnclosingObject(), ((AnnotatorContainer)oc).getObject(), es, 0, "." + fileType);
    	} else if (oc instanceof EmbedderContainer) {
    		return getEmbedderExecutionFile(oc.getObjectOwner(), oc.getEnclosingObject(), ((EmbedderContainer)oc).getObject(), es, 0, "." + fileType);
    	} else if (oc instanceof ClustererContainer) {
    		return getClustererExecutionFile(oc.getObjectOwner(), oc.getEnclosingObject(), ((ClustererContainer)oc).getObject(), es, 0, "." + fileType);
    	} else if (oc instanceof MappingContainer) {
    		return getMappingExecutionFile(oc.getObjectOwner(), oc.getEnclosingObject(), ((MappingContainer)oc).getObject(), ((MappingContainer)oc).getMappingInstance(), es, 0, "." + fileType);
    	}
    	
    	return null;
   	}
    
    public File getMappingExecutionTrigFile(UserPrincipal currentUser, Dataset dataset, MappingDocument doc, MappingInstance mi, ExecuteState es, int shard) {
    	return getMappingExecutionFile(currentUser, dataset, doc, mi, es, shard, ".trig");
   	}

    public File getAnnotatorExecutionTrigFile(UserPrincipal currentUser, Dataset dataset, AnnotatorDocument doc, ExecuteState es, int shard) {
    	return getAnnotatorExecutionFile(currentUser, dataset, doc, es, shard, ".trig");
   	}

    public File getClustererExecutionTrigFile(UserPrincipal currentUser, Dataset dataset, ClustererDocument doc, ExecuteState es, int shard) {
    	return getClustererExecutionFile(currentUser, dataset, doc, es, shard, ".trig");
   	}

    public File getAnnotatorExecutionCatalogTrigFile(UserPrincipal currentUser, Dataset dataset, AnnotatorDocument doc, ExecuteState es) {
    	return getAnnotatorExecutionFile(currentUser, dataset, doc, es, 0, "_catalog.trig");
   	}
    
    public File getAnnotationValidationExecutionCatalogTrigFile(UserPrincipal currentUser, Dataset dataset, AnnotationValidation doc, ExecuteState es) {
    	return getAnnotationValidationExecutionFile(currentUser, dataset, doc, es, 0, "_catalog.trig");
   	}
    
    public File getMappingExecutionTxtFile(UserPrincipal currentUser, Dataset dataset, MappingDocument doc, MappingInstance mi, ExecuteState es, int shard) {
    	return getMappingExecutionFile(currentUser, dataset, doc, mi, es, shard, ".txt");
   	}

    public File getMappingExecutionZipFile(UserPrincipal currentUser, Dataset dataset, MappingDocument doc, MappingInstance mi, ExecuteState es) {
    	return getMappingExecutionFile(currentUser, dataset, doc, mi, es, 0, ".zip");
   	}

    public File getAnnotatorExecutionZipFile(UserPrincipal currentUser, Dataset dataset, AnnotatorDocument doc, ExecuteState es) {
    	return getAnnotatorExecutionFile(currentUser, dataset, doc, es, 0, ".zip");
   	}

    private File getMappingExecutionFile(UserPrincipal currentUser, Dataset dataset, MappingDocument doc, MappingInstance mi, ExecuteState es, int shard, String extension) {
		String datasetFolder = getMappingsExecutionDatasetFolder(currentUser, dataset);
		String fileName = buildFileName(doc, mi, es, shard) + extension;
		
		File file = new File(datasetFolder + "/" + fileName); 
		if (file.exists()) {
			return file;
		}
		
		//for compatibility 2
		fileName = buildNoTimeStampFileName(doc, mi, shard) + extension;
		file = new File(datasetFolder + "/" + fileName);
		if (file.exists()) {
			return file;
		}

		//for compatibility 1
		file = new File(getMappingsFolder(currentUser) + "/" + fileName);
		if (file.exists()) {
			return file;
		}
		
		return null;
	}
    
    private File getAnnotatorExecutionFile(UserPrincipal currentUser, Dataset dataset, AnnotatorDocument doc, ExecuteState es, int shard, String extension) {
 		String datasetFolder = getAnnotationsExecutionDatasetFolder(currentUser, dataset);
 		String fileName = buildFileName(doc, es, shard) + extension;
 		
 		File file = new File(datasetFolder + "/" + fileName);
 		if (file.exists()) {
 			return file;
 		}
 		
// 		//for compatibility 2
 		fileName = buildNoTimeStampFileName(doc, shard) + extension;

 		//for compatibility 1
 		file = new File(getAnnotationsFolder(currentUser) + "/" + fileName);
 		if (file.exists()) {
 			return file;
 		}
 		
 		return null;
 	}

    private File getClustererExecutionFile(UserPrincipal currentUser, Dataset dataset, ClustererDocument doc, ExecuteState es, int shard, String extension) {
 		String datasetFolder = getClusteringsExecutionDatasetFolder(currentUser, dataset);
 		String fileName = buildFileName(doc, es, shard) + extension;
 		
 		File file = new File(datasetFolder + "/" + fileName);
 		if (file.exists()) {
 			return file;
 		}
 		
// 		//for compatibility 2
 		fileName = buildNoTimeStampFileName(doc, shard) + extension;

 		//for compatibility 1
 		file = new File(getClusterersFolder(currentUser) + "/" + fileName);
 		if (file.exists()) {
 			return file;
 		}
 		
 		return null;
 	}

    private File getEmbedderExecutionFile(UserPrincipal currentUser, Dataset dataset, EmbedderDocument doc, ExecuteState es, int shard, String extension) {
 		String datasetFolder = getEmbeddingsExecutionDatasetFolder(currentUser, dataset);
 		String fileName = buildFileName(doc, es, shard) + extension;
 		
 		File file = new File(datasetFolder + "/" + fileName); 
 		if (file.exists()) {
 			return file;
 		}
 		
// 		//for compatibility 2
 		fileName = buildNoTimeStampFileName(doc, shard) + extension;

 		//for compatibility 1
 		file = new File(getEmbeddingsFolder(currentUser) + "/" + fileName);
 		if (file.exists()) {
 			return file;
 		}
 		
 		return null;
 	}
    
    private File getAnnotationValidationExecutionFile(UserPrincipal currentUser, Dataset dataset, AnnotationValidation fav, ExecuteState es, int shard, String extension) {
 		String datasetFolder = getAnnotationsExecutionDatasetFolder(currentUser, dataset);
 		String fileName = buildFileName(fav, es, shard) + extension;
 		
 		File file = new File(datasetFolder + "/" + fileName); 
 		if (file.exists()) {
 			return file;
 		}
 		
// 		//for compatibility 2
 		fileName = buildNoTimeStampFileName(fav, shard) + extension;

 		//for compatibility 1
 		file = new File(getAnnotationsFolder(currentUser) + "/" + fileName);
 		if (file.exists()) {
 			return file;
 		}
 		
 		return null;
 	}

//     private File getMappingExecutionFile(UserPrincipal currentUser, Dataset dataset, MappingDocument doc, MappingInstanceResponse mi, ExecuteState es, int shard, String extension) {
//		String datasetFolder = getMappingsExecutionDatasetFolder(currentUser, dataset);
//		String fileName = buildFileName(doc, mi, es, shard) + extension;
//		
//		File file = new File(datasetFolder + "/" + fileName); 
//		if (file.exists()) {
//			return file;
//		}
//		
//		//for compatibility 2
//		fileName = buildNoTimeStampFileName(doc, mi, shard) + extension;
//		file = new File(datasetFolder + "/" + fileName);
//		if (file.exists()) {
//			return file;
//		}
//
//		//for compatibility 1
//		file = new File(getMappingsFolder(currentUser) + "/" + fileName);
//		if (file.exists()) {
//			return file;
//		}
//		
//		return null;
//	}
     
    public FileSystemRDFOutputHandler createDatasetSaturateOutputHandler(UserPrincipal currentUser, Dataset dataset, int shardSize) {
    	String mf = getMappingsFolder(currentUser);
//		if (!new File(df).exists()) {
//			new File(df).mkdir();
//		}
		
		return new FileSystemRDFOutputHandler(mf, dataset.getUuid() + "-owl-sameAs-saturate", shardSize);
    }
    
    public File getDatasetSaturateTrigFile(UserPrincipal currentUser, Dataset dataset, int shard) {
    	File f = new File(getMappingsFolder(currentUser), dataset.getUuid() + "-owl-sameAs-saturate" + (shard == 0 ? "" : "_#" + shard) + ".trig");
    	if (!f.exists()) {
    		return null;
    	}
    	
    	return f;
    }

    public boolean deleteUploadedFilesFolderIfEmpty(UserPrincipal currentUser, Dataset dataset, FileDocument fd) {
    	File folder = new File(getUploadsFolder(currentUser) + "/" + dataset.getUuid(), fd.getUuid());
    	
		if (folder.exists()) {
			if (folder.isDirectory() && folder.list().length == 0) {
				boolean ok = folder.delete();
				if (ok) {
					logger.info("Deleted folder " + folder.getAbsolutePath());
				}
				
				folder = new File(getUploadsFolder(currentUser) + "/" + dataset.getUuid());
				if (folder.exists() && folder.isDirectory() && folder.list().length == 0) {
					ok = folder.delete();
					if (ok) {
						logger.info("Deleted folder " + folder.getAbsolutePath());
					}
				}
			}
		
			return true;
		}    
		
		if (!folder.exists()) {
			folder = new File(getUploadsFolder(currentUser), fd.getUuid());
		}

		if (!folder.exists()) {
			folder = new File(getUploadsFolder(currentUser), fd.getId().toString());
		}
	
		if (folder.exists()) { 
			for (File ff : folder.listFiles()) {
				ff.delete();
			}
			folder.delete();
		
			return true;
		} 
    	//for compatibility -->
		
		return false;
    }
    
	public File getUploadedFilesFolder(UserPrincipal currentUser, Dataset dataset, FileDocument fd) {
		File folder = new File(getUploadsFolder(currentUser) + "/" + dataset.getUuid(), fd.getUuid());
    	
		//for compatibility <--
		if (!folder.exists()) {
			folder = new File(getUploadsFolder(currentUser), fd.getUuid());
		}

    	if (!folder.exists()) {
			folder = new File(getUploadsFolder(currentUser), fd.getId().toString());
		}
    	
    	if (folder.exists()) {
    		return folder;
    	}
    	
    	//for compatibility -->
    	
    	return null;
	}
	
	public List<File> getUploadedFiles(UserPrincipal currentUser, Dataset dataset, FileDocument fd, ExecuteState es) {
		File folder = new File(getUploadsFolder(currentUser) + "/" + dataset.getUuid(), fd.getUuid());
    	
		//for compatibility <--
		if (!folder.exists()) {
			folder = new File(getUploadsFolder(currentUser), fd.getUuid());
		}

    	
    	if (!folder.exists()) {
			folder = new File(getUploadsFolder(currentUser), fd.getId().toString());
		}
    	//for compatibility -->
    	
    	if (folder.exists()) { 
    		List<File> res = new ArrayList<>();

    		if (es == null) {
	    		for (File f : folder.listFiles()) {
	    			if (f.isFile() && !f.getName().endsWith(".zip")) {
	    				res.add(f);
	    			}
	    		}
    		} else {
    			String prefix = es.getExecuteStartStamp();
	    		for (File f : folder.listFiles()) {
	    			if (f.isFile() && f.getName().startsWith(prefix) && !f.getName().endsWith(".zip")) {
	    				res.add(f);
	    			}
	    		}
    		}
    		return res;
    	} 
    	
    	return null;
	}
	
    public File getUploadedZipFile(UserPrincipal currentUser, Dataset dataset, FileDocument fd, FileExecuteState es) {
    	String file = es.getFileName();
    	int pos = file.lastIndexOf(".");
    	if (pos >= 0) {
    		file = file.substring(0, pos);
    	}
    	
    	return new File(getUploadsFolder(currentUser) + "/" + dataset.getUuid() + "/" + fd.getUuid(), es.getExecuteStartStamp() + "_" + file  + ".zip");
    }
	
    public List<String> saveUploadedFile(UserPrincipal currentUser, Dataset dataset, FileDocument fd, MultipartFile file) throws IllegalStateException, IOException {
//    	deleteUploadedFiles(currentUser, dataset, fd);

    	File folder = new File(getUploadsFolder(currentUser) + "/" + dataset.getUuid());
    	if (!folder.exists()) {
    		folder.mkdir();
    	}
    	
    	folder = new File(getUploadsFolder(currentUser) + "/" + dataset.getUuid(), fd.getUuid());
    	if (!folder.exists()) {
    		folder.mkdir();
    	}
    	
    	List<String> res = new ArrayList<>();

    	String stamp = fd.getExecuteState(fileSystemConfiguration.getId()).getExecuteStartStamp();

		File newFile = new File(folder, stamp + "_" + file.getOriginalFilename());
		file.transferTo(newFile);
		if (newFile.getName().endsWith(".zip")) {
	        byte[] buffer = new byte[2048];
	        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(newFile))) {
		        ZipEntry zipEntry = zis.getNextEntry();
		        while (zipEntry != null) {
		        	File destFile = new File(folder, stamp + "_" + zipEntry.getName());
		        	res.add(destFile.getName());
		        	
		        	try (FileOutputStream fos = new FileOutputStream(destFile)) {
			            int len;
			            while ((len = zis.read(buffer)) > 0) {
			                fos.write(buffer, 0, len);
			            }
		        	}
		        	
		            zipEntry = zis.getNextEntry();
		        }
		        zis.closeEntry();
	        }
	        newFile.delete();
	    } else	if (newFile.getName().endsWith(".bz2")) {
	    	byte[] buffer = new byte[2048];
	        try (BZip2CompressorInputStream input = new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(newFile)))) {
	        	File destFile = new File(folder, stamp + "_" + newFile.getName().substring(0, newFile.getName().length()-4));
	        	res.add(destFile.getName());
	        	
	       		try (FileOutputStream fos = new FileOutputStream(destFile)) {
	       			int len;
		            
	       			while ((len = input.read(buffer)) > 0) {
	       				fos.write(buffer, 0, len);
		            }
	        	}
	        }
	        newFile.delete();
	    } else {
	    	res.add(newFile.getName());
	    }
		
		return res;
    }
    
    

	public List<File> getUploadedFiles(UserPrincipal currentUser, MappingDocument md) {
    	File folder = new File(getUploadsFolder(currentUser), md.getUuid());

    	//for compatibility <--
    	if (!folder.exists()) {
			folder = new File(getUploadsFolder(currentUser), md.getId().toString());
		}
    	//for compatibility -->
    	
    	if (folder.exists()) { 
    		List<File> res = new ArrayList<>();
    		for (File f : folder.listFiles()) {
    			if (f.isFile()) {
    				res.add(f);
    			}
    		}
    		return res;
    	} 
    	
    	return null;
	}
	
    public void saveMappingD2RMLFile(UserPrincipal currentUser, MappingDocument doc, MultipartFile file) throws IOException {
		File newFile = new File(getUploadsFolder(currentUser), doc.getUuid() + ".ttl");
		file.transferTo(newFile);
    }  
    
    public String saveAttachment(UserPrincipal currentUser, MappingDocument doc, MultipartFile file) throws IOException {
		File folder = new File(getUploadsFolder(currentUser), doc.getUuid());

		if (!folder.exists()) {
			logger.info("Creating folder " + folder);
			folder.mkdir();
		}

		File ffile = new File(folder, file.getOriginalFilename());
		
		logger.info("Saving " + ffile.getAbsolutePath() + " (size: " + file.getSize() + ")");
		
		file.transferTo(ffile);
		
		return file.getOriginalFilename();
    }
    
    public File getAttachmentPath(UserPrincipal currentUser, MappingDocument doc, String filename) {
    	return new File(getUploadsFolder(currentUser), doc.getUuid() + "/" + filename);
    }

    public File getAttachmentPath(UserPrincipal currentUser, MappingDocument doc, MappingInstance mi, String filename) {
    	return new File(getUploadsFolder(currentUser), doc.getUuid() + "/" + mi.getId() + "_" + filename);
    }

    
    public String saveAttachment(UserPrincipal currentUser, MappingDocument doc, MappingInstance mi, MultipartFile file) throws IOException {
		File folder = new File(getUploadsFolder(currentUser), doc.getUuid());

		if (!folder.exists()) {
			logger.info("Creating folder " + folder);
			folder.mkdir();
		}

		File ffile = new File(folder, mi.getId() + "_" + file.getOriginalFilename());
		
		logger.info("Saving " + ffile + " (size: " + file.getSize() + ")");
		
		file.transferTo(ffile);
		
		return file.getOriginalFilename();
    }
    
    public boolean deleteAttachment(UserPrincipal currentUser, MappingDocument doc, MappingInstance mi, String filename) throws IOException {
		File folder = new File(getUploadsFolder(currentUser), doc.getUuid());

		if (!folder.exists()) {
			return false;
		}

		File file = new File(folder, (mi != null ? (mi.getId() + "_") : "") + filename);
		
		if (!file.exists()) {
			return false;
		}
		
		
		logger.info("Deleting " + file);
		
		boolean res = file.delete();
		
		if (folder.listFiles().length == 0) {
			folder.delete();
		}
		
		return res;
    }
    
    public File getAttachment(UserPrincipal currentUser, MappingDocument doc, MappingInstance mi, String filename) throws IOException {
		File folder = new File(getUploadsFolder(currentUser), doc.getUuid());

		if (!folder.exists()) {
			return null;
		}

		File file = new File(folder, (mi != null ? (mi.getId() + "_") : "") + filename);
		
		if (!file.exists()) {
			return null;
		}
		
		return file;
    }
    
	public void checkPaths(UserPrincipal currentUser) throws Exception {
		File df = new File(fileSystemConfiguration.getDataFolder());
		
		if (!df.exists()) {
			throw new Exception("Data folder " + df.getAbsolutePath() + " does not exits");
		}

		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser));
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "mappings");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "mappings/executions");
		df.mkdir();
		
		
//		for (File d : df.listFiles()) {
//			if (d.isDirectory()) {
//				if (!datasetRepository.findByUuid(d.getName()).isPresent()) {
//					logger.warn("Orfan folder " + d + " (no dataset)");
//				}
//			}
//		}
		
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "mappings/uploaded-files");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "annotations");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "annotations/executions");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "annotations/manual");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "vocabularizers");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "vocabularizers/executions");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "datasets");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "datasets/distributions");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "embeddings");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "embeddings/executions");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "clusterings");
		df.mkdir();
		df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + "clusterings/executions");
		df.mkdir();
		
	}
}
