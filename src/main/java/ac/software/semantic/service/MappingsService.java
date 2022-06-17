
package ac.software.semantic.service;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import ac.software.semantic.controller.AsyncUtils;
import ac.software.semantic.controller.ExecuteMonitor;
import ac.software.semantic.controller.SSEController;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.DependencyBinding;
import ac.software.semantic.model.ExecuteNotificationObject;
import ac.software.semantic.model.ExecuteState;
import ac.software.semantic.model.ExecutionInfo;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.MappingState;
import ac.software.semantic.model.MappingType;
import ac.software.semantic.model.NotificationObject;
import ac.software.semantic.model.ParameterBinding;
import ac.software.semantic.model.PublishState;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.payload.MappingResponse;
import ac.software.semantic.repository.MappingRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.util.SerializationTransformation;
import edu.ntua.isci.ac.common.utils.IOUtils;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.monitor.FileSystemOutputHandler;
import edu.ntua.isci.ac.d2rml.parser.Parser;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;

@Service
public class MappingsService {

	Logger logger = LoggerFactory.getLogger(MappingsService.class);

	@Autowired
	private ModelMapper modelMapper;

	@Autowired
	private MappingRepository mappingsRepository;

	@Autowired
	DatasetRepository datasetRepository;

//	@Autowired
//	private ExecutionsRepository executionsRepository;

	@Value("${mapping.execution.folder}")
	private String mappingsFolder;

	@Value("${mapping.uploaded-files.folder}")
	private String uploadsFolder;

	@Value("${mapping.temp.folder}")
	private String tempFolder;

	@Value("${d2rml.execute.safe}")
	private boolean safeExecute;

	@Value("${d2rml.execute.shard-size}")
	private int shardSize;
	
	@Value("${d2rml.extract.min-size:0}")
	private long extractMinSize; 
	
	@Value("${d2rml.extract.temp-folder:null}")
	private String extractTempFolder;

	@Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfiguration;

	@Autowired
	@Qualifier("filesystem-configuration")
	private FileSystemConfiguration fileSystemConfiguration;

	@Autowired
	VirtuosoJDBC virtuosoJDBC;
	
	public MappingDocument create(UserPrincipal currentUser, String datasetId, String type, String name, String d2rml, List<String> params, String fileName) {
		MappingDocument map = new MappingDocument();
		map.setUserId(new ObjectId(currentUser.getId()));
		map.setDatasetId(new ObjectId(datasetId));
		map.setType(MappingType.get(type));
		map.setUuid(UUID.randomUUID().toString());
		map.setName(name);
		map.setD2RML(d2rml);
		map.setParameters(params);
		map.setFileName(fileName);
		
		map = mappingsRepository.save(map);

		return map;
		
	}

	public MappingDocument create(UserPrincipal currentUser, String datasetId, String type, String name, String d2rml, List<String> params, String fileName, String uuid) {
		MappingDocument map = new MappingDocument();
		map.setUserId(new ObjectId(currentUser.getId()));
		map.setDatasetId(new ObjectId(datasetId));
		map.setType(MappingType.get(type));
		map.setUuid(uuid);
		map.setName(name);
		map.setD2RML(d2rml);
		map.setParameters(params);
		map.setFileName(fileName);

		map = mappingsRepository.save(map);

		return map;

	}

	public boolean updateMapping(UserPrincipal currentUser, String id, String name, String d2rml, List<String> parameters, String fileName) {

		Optional<MappingDocument> entry = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		if (entry.isPresent()) {
			MappingDocument doc = entry.get();
			if (name != null) {
				doc.setName(name);
			}
			
			if (d2rml != null) {
				doc.setD2RML(d2rml);
			}
			
			if (fileName != null) {
				doc.setFileName(fileName);
			}
			
			doc.setParameters(parameters);

			mappingsRepository.save(doc);
			
			return true;
		}

		return false;
	}
	public List<MappingResponse> getMappings(UserPrincipal currentUser, String datasetId) {

		List<MappingDocument> docs = mappingsRepository.findByDatasetIdAndUserId(new ObjectId(datasetId),
				new ObjectId(currentUser.getId()));

		List<MappingResponse> response = docs.stream().map(doc -> modelMapper.mapping2MappingResponse(virtuosoConfiguration.values(), doc, currentUser))
				.collect(Collectors.toList());

		return response;
	}

	public List<MappingDocument> getHeaderMappings(UserPrincipal currentUser, ObjectId datasetId) {

		List<MappingDocument> docs = mappingsRepository.findByUserIdAndDatasetIdAndType(new ObjectId(currentUser.getId()), datasetId, "HEADER");

		return docs;
	}

	public boolean deleteMapping(UserPrincipal currentUser, String mappingId) {

		Optional<MappingDocument> entry = mappingsRepository.findByIdAndUserId(new ObjectId(mappingId), new ObjectId(currentUser.getId()));

		if (!entry.isPresent()) {
			return false;
		}
		MappingDocument doc = entry.get();

		// Clear execution of all mapping instances of the mapping
		List<MappingInstance> instanceList = doc.getInstances();
		boolean hasParameters = !doc.getParameters().isEmpty();
		if (hasParameters) {
			for (MappingInstance instance : instanceList) {
				clearExecution(currentUser, mappingId, instance.getId().toString());
			}
		}
		else {
			clearExecution(currentUser, mappingId, null);
		}

		// Now delete the mapping
		mappingsRepository.delete(doc);

		return true;
	}
	


	public boolean deleteParameterBinding(UserPrincipal currentUser, String id, String instanceId) throws IOException {

		Optional<MappingDocument> entry = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		if (!entry.isPresent()) {
			return false;
		}

		clearExecution(currentUser, id, instanceId);

		MappingDocument doc = entry.get();
		
		List<MappingInstance> list = doc.getInstances();
		for (int k = 0; k < list.size(); k++) {
			MappingInstance mi = list.get(k);
			if (mi.getId().toString().equals(instanceId)) {
				list.remove(k);
				break;
			}
		}

		mappingsRepository.save(doc);

		return true;
	}

	public Optional<MappingResponse> getMapping(UserPrincipal currentUser, String mappingId) {

		Optional<MappingDocument> doc = mappingsRepository.findByIdAndUserId(new ObjectId(mappingId),
				new ObjectId(currentUser.getId()));
		if (doc.isPresent()) {
			return Optional.of(modelMapper.mapping2MappingResponse(virtuosoConfiguration.values(), doc.get(), currentUser));
		} else {
			return Optional.empty();
		}
	}

	
	public Optional<String> getLastExecution(UserPrincipal currentUser, String id, String instanceId)
			throws IOException {

		Optional<MappingDocument> mappingOpt = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		if (!mappingOpt.isPresent()) {
			return Optional.empty();
		}

		MappingDocument doc = mappingOpt.get();

		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(doc.getDatasetId(), new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return Optional.empty();
		}
		
		Dataset dataset = datasetOpt.get();
		
		MappingInstance mi = getMappingInstance(doc, instanceId);

		String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder + dataset.getUuid() + "/";
		String fileName = doc.getUuid() + (instanceId == null ? "" : "_" + mi.getId().toString()) + ".trig";

		File fileToRead = new File(datasetFolder + fileName); 
		if (!fileToRead.exists()) {
			//for compatibility
			fileToRead = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder + fileName); 
		}

		if (fileToRead.exists()) {
//	     	String filex = new String(Files.readAllBytes(path));
		
			String file = AsyncUtils.readFileBeginning(Paths.get(fileToRead.getAbsolutePath()));
			return Optional.of(file);
		} else {
			logger.error("File " + fileToRead + " does not exist." );
			return Optional.empty();
		}

		
	}

	
	public Optional<String> downloadLastExecution(UserPrincipal currentUser, String id, String instanceId) throws IOException {

		Optional<MappingDocument> mappingOpt = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		if (!mappingOpt.isPresent()) {
			return Optional.empty();
		}

		MappingDocument doc = mappingOpt.get();

		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(doc.getDatasetId(), new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return Optional.empty();
		}
		
		Dataset dataset = datasetOpt.get();
		
		MappingInstance mi = getMappingInstance(doc, instanceId);
		
		ExecuteState  es = mi.getExecuteState(fileSystemConfiguration.getId());

		String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder + dataset.getUuid() + "/";
		String fileName = doc.getUuid() + (instanceId == null ? "" : "_" + mi.getId().toString()) + ".zip";

		String file = datasetFolder + fileName;

		if (es.getExecuteState() == MappingState.EXECUTED) {
			if (new File(file).exists()) {
				
			} else {
				// for compatibility
				datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder;
				file = datasetFolder + fileName;
				
				if (!new File(file).exists()) {
					zipExecution(datasetFolder, doc, instanceId, mi, es.getExecuteShards() == 0 ? 1 : es.getExecuteShards());
				}
			}			
			
			return Optional.of(file);
		}
		
		return Optional.empty();
	}
	
	private MappingInstance getMappingInstance(MappingDocument doc, String instanceId) {
		MappingInstance mi = null;
		if (instanceId == null) {
			mi = doc.getInstances().get(0);
		} else {
			for (MappingInstance mix : doc.getInstances()) {
				if (mix.getId().toString().equals(instanceId)) {
					mi = mix;
					break;
				}
			}
		}
		
		return mi;
	}
	
	public boolean clearExecution(UserPrincipal currentUser, String id, String instanceId) {

		Optional<MappingDocument> mappingOpt = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		if (!mappingOpt.isPresent()) {
			return false;
		}

		MappingDocument doc = mappingOpt.get();

		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(doc.getDatasetId(), new ObjectId(currentUser.getId()));
		
		if (!datasetOpt.isPresent()) {
			return false;
		}
		
		Dataset dataset = datasetOpt.get();
		String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder + dataset.getUuid() + "/";

		MappingInstance mi = getMappingInstance(doc, instanceId);
		ExecuteState es = mi.getExecuteState(fileSystemConfiguration.getId());

		for (int i = 0; i < es.getExecuteShards(); i++) {
			String fileName =  doc.getUuid() + (instanceId != null ? "_" + mi.getId().toString() : "") + (i == 0 ? "" : "_#" + i) + ".trig";
			
			try {
				File f = new File(datasetFolder + fileName);
				boolean ok = false;
				if (f.exists()) {
					ok = f.delete();
					if (ok) {
						logger.info("Deleted " + f.getName());
					}
				} else {
					// for compatibility
					f = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder + fileName);
					if (f.exists()) {
						ok = f.delete();
						if (ok) {
							logger.info("Deleted " + f.getName());
						}
					}
				}
				if (!ok) {
					logger.warn("Failed to delete " + f.getName());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
			
		try {
			String fileName =  doc.getUuid() + (instanceId != null ? "_" + mi.getId().toString() : "") + ".zip";
			File f = new File(datasetFolder + fileName);
			boolean ok = false;
			if (f.exists()) {
				ok = f.delete();
				if (ok) {
					logger.info("Deleted file " + f.getAbsolutePath());
				}
			} else {
				// for compatibility
				f = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder + fileName);
				if (f.exists()) {
					ok = f.delete();
					if (ok) {
						logger.info("Deleted file " + f.getAbsolutePath());
					}
				}
			}
			if (!ok) {
				logger.warn("Failed to delete " + f.getAbsolutePath());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		File ff = new File(datasetFolder);
		if (ff.exists() && ff.isDirectory() && ff.list().length == 0) {
			boolean ok = ff.delete();
			if (ok) {
				logger.info("Deleted folder " + ff.getAbsolutePath());
			}
			
		}
		
		mi.deleteExecuteState(fileSystemConfiguration.getId());
		mappingsRepository.save(doc);

		return true;
	}	
	
	//very experimental and problematic.
	public boolean unpublish(UserPrincipal currentUser, String id, String instanceId) throws Exception {

		Optional<MappingDocument> mappingOpt = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		if (!mappingOpt.isPresent()) {
			return false;
		}

		MappingDocument mapping = mappingOpt.get();

		Optional<Dataset> dOpt = datasetRepository.findByIdAndUserId(mapping.getDatasetId(), new ObjectId(currentUser.getId()));
		
		if (!dOpt.isPresent()) {
			return false;
		}
		
		Dataset dataset = dOpt.get();
		
		PublishState ps = null;
		String virtuoso = null;

		for (VirtuosoConfiguration vc : virtuosoConfiguration.values()) {
			ps = dataset.checkPublishState(vc.getId());
			if (ps != null) {
				virtuoso = vc.getName();
				break;
			}
		}
		
		if (ps == null) {
			return false;
		}
		
		
		MappingInstance mi = getMappingInstance(mapping, instanceId);
		PublishState mappingPs = mi.checkPublishState(virtuosoConfiguration.get(virtuoso).getId());
		
		if (mappingPs == null) {
			return false;
		}
		
		mappingPs.setPublishState(DatasetState.UNPUBLISHING);
		mappingsRepository.save(mapping);
		
		virtuosoJDBC.unpublishMapping(currentUser, virtuoso, dataset, mapping, mi);
		
		mappingPs.setPublishState(DatasetState.UNPUBLISHED);
		mappingsRepository.save(mapping);
			
			
		return true;
	}	
	
	public MappingInstance createParameterBinding(UserPrincipal currentUser, String id, List<ParameterBinding> bindings) {

		Optional<MappingDocument> entry = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		MappingDocument doc = entry.get();
		MappingInstance mi = doc.addInstance(bindings);

		mappingsRepository.save(doc);

		return mi;
	}
	
	public boolean executeMapping(UserPrincipal currentUser, String id, String instanceId, ApplicationEventPublisher applicationEventPublisher) {

		Optional<MappingDocument> mappingOpt = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		if (!mappingOpt.isPresent()) {
			return false;
		}

		MappingDocument doc = mappingOpt.get();
		
		Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(doc.getDatasetId(), new ObjectId(currentUser.getId()));

		if (!datasetOpt.isPresent()) {
			return false;
		}
		
		for (DependencyBinding db : doc.getDependencies()) {
			for(ObjectId dependecyId : db.getValue()) {
				Optional<MappingDocument> dmappingOpt = mappingsRepository.findByIdAndUserId(dependecyId, new ObjectId(currentUser.getId()));
				if (!dmappingOpt.isPresent() || !dmappingOpt.get().isExecuted(fileSystemConfiguration.getId())) {
					return false;
				}	
			}
		}

		Dataset dataset = datasetOpt.get();

		MappingInstance mi = getMappingInstance(doc, instanceId);

		ExecuteState es = mi.getExecuteState(fileSystemConfiguration.getId());

		String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder + dataset.getUuid() + "/";
		
		// Clearing old files
		clearExecution(currentUser, id, instanceId);
		
//		if (es.getExecuteState() == MappingState.EXECUTED) {
//			boolean hi = !doc.getParameters().isEmpty();
//			
//			//for compatibility
//			for (int i = 0; i < es.getExecuteShards(); i++) {
//				(new File(fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder + doc.getUuid()
//						+ (hi ? "_" + mi.getId().toString() : "") + (i == 0 ? "" : "_#" + i) + ".trig")).delete();
//			}
//			//new version
//			if (new File(datasetFolder).exists()) {
//				for (int i = 0; i < es.getExecuteShards(); i++) {
//					(new File(datasetFolder + doc.getUuid()
//							+ (hi ? "_" + mi.getId().toString() : "") + (i == 0 ? "" : "_#" + i) + ".trig")).delete();
//				}
//			}
//		}

		Date executeStart = new Date(System.currentTimeMillis());

		es.setExecuteState(MappingState.EXECUTING);
		es.setExecuteStartedAt(executeStart);
		es.setExecuteShards(0);
		es.setCount(0);

		mappingsRepository.save(doc);

		if (!new File(datasetFolder).exists()) {
			new File(datasetFolder).mkdir();
		}
				
		try (FileSystemOutputHandler outhandler = new FileSystemOutputHandler(
				datasetFolder,
				doc.getUuid() + (instanceId == null ? "" : "_" + mi.getId().toString()), shardSize)) {

			Map<String, Object> fileMap = new HashMap<>();

			File f = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder + doc.getUuid());
			if (f.exists() && f.isDirectory()) {
				for (File df : f.listFiles()) {
					if (df.isFile()) {
						fileMap.put(df.getName(), Paths.get(df.getAbsolutePath()));
					}
				}
			}

			// <-- for compatibility 
			f = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder + id);
			if (f.exists() && f.isDirectory()) {
				for (File df : f.listFiles()) {
					if (df.isFile()) {
						fileMap.put(df.getName(), Paths.get(df.getAbsolutePath()));
					}
				}
			}
			// for compatibility -->
			
			Map<String, Object> params = new HashMap<>();
			if (!fileMap.isEmpty()) {
				params.put("files", fileMap);
				params.put("vpath", true);
			}

			Map<String, Object> execParams = new HashMap<>();
			for (ParameterBinding binding : mi.getBinding()) {
				execParams.put(binding.getName(), binding.getValue());
			}

			// create dependency datasets
			for (DependencyBinding db : doc.getDependencies()) {
				org.apache.jena.query.Dataset paramDataset = DatasetFactory.create();
				logger.info("Creating tmp dataset " + db.getName());

				for (ObjectId dependecyId : db.getValue()) {
					Optional<MappingDocument> dmappingOpt = mappingsRepository.findByIdAndUserId(dependecyId, new ObjectId(currentUser.getId()));
					if (!dmappingOpt.isPresent() || !dmappingOpt.get().isExecuted(fileSystemConfiguration.getId())) {
						return false;
					}	
				}
				
				executionResultsToModel(paramDataset, currentUser, db.getValue());
				
				logger.info("Created tmp dataset " + db.getName());
				
				execParams.put(db.getName(), paramDataset);
			}

//			Executor exec = new Executor(ts, tempFolder != null && tempFolder.length() > 0 ? fileSystemConfiguration.getUserDataFolder(currentUser) + tempFolder: null, safeExecute);
			Executor exec = new Executor(outhandler, safeExecute);
			
			File df = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + extractTempFolder);
			if (!df.exists()) {
				logger.info("Created tmp folder " + fileSystemConfiguration.getUserDataFolder(currentUser) + extractTempFolder);
				df.mkdir();
			}
				
			exec.configureFileExtraction(extractMinSize, fileSystemConfiguration.getUserDataFolder(currentUser) + extractTempFolder, 0);

			try (ExecuteMonitor em = new ExecuteMonitor("mapping", id, instanceId, applicationEventPublisher)) {
				exec.setMonitor(em);

				D2RMLModel d2rml = new SerializationTransformation().XtoD2RMLModel(doc.getD2RML(), params);

				SSEController.send("mapping", applicationEventPublisher, this, new ExecuteNotificationObject(id,
						instanceId, ExecutionInfo.createStructure(d2rml), executeStart));

				logger.info("Mapping started -- id: " + id + (instanceId != null ? ("_" + instanceId) : ""));

				exec.execute(d2rml, execParams);

				Date executeFinish = new Date(System.currentTimeMillis());

				es.setExecuteState(MappingState.EXECUTED);
				es.setExecuteCompletedAt(executeFinish);
				es.setExecuteShards(outhandler.getShards());
				es.setCount(outhandler.getTotalItems());

				mappingsRepository.save(doc);

				SSEController.send("mapping", applicationEventPublisher, this,
						new NotificationObject("execute", MappingState.EXECUTED.toString(), id, instanceId,
								executeStart, executeFinish, outhandler.getTotalItems()));

				logger.info("Mapping executed -- id: " + id + (instanceId != null ? ("_" + instanceId) : "")
						+ ", shards: " + outhandler.getShards());

				try {
					zipExecution(datasetFolder, doc, instanceId, mi, outhandler.getShards());
				} catch (Exception ex) {
					ex.printStackTrace();
					
					logger.info("Zipping mapping execution failed -- id: " + id + (instanceId != null ? ("_" + instanceId) : ""));
				}
				
				return true;

			} catch (Exception ex) {
				ex.printStackTrace();

				logger.info("Mapping failed -- id: " + id + (instanceId != null ? ("_" + instanceId) : ""));

				exec.getMonitor().currentConfigurationFailed();

				throw ex;
			}
		} catch (Exception ex) {
			ex.printStackTrace();

			es.setExecuteState(MappingState.EXECUTION_FAILED);

			mappingsRepository.save(doc);

			SSEController.send("mapping", applicationEventPublisher, this, new NotificationObject("execute",
					MappingState.EXECUTION_FAILED.toString(), id, instanceId, null, null, ex.getMessage()));

			return false;
		}

	}

	private void zipExecution(String datasetFolder, MappingDocument doc, String instanceId, MappingInstance mi, int shards) throws IOException {
		
		try (FileOutputStream fos = new FileOutputStream(datasetFolder + doc.getUuid() + (instanceId == null ? "": "_" + mi.getId().toString())  + ".zip");
				ZipOutputStream zipOut = new ZipOutputStream(fos)) {
	        for (int i = 0; i < shards; i++) {
	            File fileToZip = new File(datasetFolder + doc.getUuid() + (instanceId == null ? "": "_" + mi.getId().toString()) + (i == 0 ? "" : "_#" + i) + ".trig");
	            
	            try (FileInputStream fis = new FileInputStream(fileToZip)) {
		            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
		            zipOut.putNextEntry(zipEntry);
		 
		            byte[] bytes = new byte[1024];
		            int length;
		            while((length = fis.read(bytes)) >= 0) {
		                zipOut.write(bytes, 0, length);
		            }
	            }
	        }
        }
	}
	
	
	public void executionResultsToModel(org.apache.jena.query.Dataset ds, UserPrincipal currentUser, List<ObjectId> ids) throws IOException {
		if (ds == null) {
			return;
		}
		for (ObjectId id : ids) {

			Optional<MappingDocument> mappingOpt = mappingsRepository.findByIdAndUserId(id, new ObjectId(currentUser.getId()));

			if (!mappingOpt.isPresent()) {
				continue;
			}

			MappingDocument doc = mappingOpt.get();
			
			Optional<Dataset> datasetOpt = datasetRepository.findByIdAndUserId(doc.getDatasetId(), new ObjectId(currentUser.getId()));
	
			if (!datasetOpt.isPresent()) {
				continue;
			}
			
			Dataset dataset	= datasetOpt.get();
			
			boolean hasParams = doc.getParameters().size() != 0;
			
			for (MappingInstance mi : doc.getInstances()) {
				
				ExecuteState  es = mi.getExecuteState(fileSystemConfiguration.getId());
	
				if (es.getExecuteState() == MappingState.EXECUTED) {
	
					String datasetFolder = fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder + dataset.getUuid() + "/";
		
			        for (int i = 0; i < es.getExecuteShards(); i++) {
			            String file = "file:/" + datasetFolder + doc.getUuid() + (!hasParams ? "": "_" + mi.getId().toString()) + (i == 0 ? "" : "_#" + i) + ".trig";
			        	logger.info("Loading file " + file);

			            RDFDataMgr.read(ds, file, Lang.TRIG);
			        }
				}
			}
		}
	}
	
	public String downloadMapping(UserPrincipal currentUser, String id) throws Exception {

		Optional<MappingDocument> doc = mappingsRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		if (doc.isPresent()) {
			Map<String, String> json = new SerializationTransformation().XtoJSONLD(doc.get().getD2RML());
			
			org.apache.jena.query.Dataset aDataset = DatasetFactory.create();
			
			try (StringReader sr = new StringReader(json.get(null))) {
				RDFDataMgr.read(aDataset, sr, null, Lang.JSONLD);
			}
			
			for (Map.Entry<String, String> entry : json.entrySet()) {
				String graph = entry.getKey();
				if (graph != null) {
					Model model = ModelFactory.createDefaultModel();

					try (StringReader sr = new StringReader(entry.getValue())) {
						RDFDataMgr.read(model, sr, null, Lang.JSONLD);
						
						aDataset.addNamedModel(graph, model);
					}
				}
			}

			StringWriter sw = new StringWriter();
			
			RDFDataMgr.write(sw, aDataset, RDFFormat.TRIG);

			StringBuffer sw2 = new StringBuffer();
//			System.out.println(sw.toString());
			String str = sw.toString();
			int newline = -1;
			String spaces = "";
			while ((newline = str.indexOf("\n")) != -1) {
				String line = str.substring(0, newline);

			    if (!line.startsWith(" ")) {
			    	spaces = "";
			    }
		    		
			    if (line.startsWith(" ") && spaces.length() == 0) {
			    	spaces = "   ";
			    }
				str = str.substring(newline + 1);
			    

				
			    int op = -1; 
				while ((op = line.indexOf("[ ")) != -1) {
					String part = line.substring(0, op + 2);
					
					sw2.append(spaces + part.replaceAll("^ +", "") + "\n");
					
					spaces += "   ";

					line = line.substring(op + 2).replaceAll("^ +", "");
					
				}
				
				int cp = -1;
				if ((cp = line.indexOf(" ] ;")) != -1) {
					spaces = spaces.substring(0, spaces.length() - 3);
					sw2.append(spaces + line.replaceAll("^ +", "") + "\n");
				} else if ((cp = line.indexOf(" ]")) != -1) {
					sw2.append(spaces + line.substring(0, cp + 1).replaceAll("^ +", "") + "\n");
					spaces = spaces.substring(0, spaces.length() - 3);
					sw2.append(spaces + line.substring(cp + 1) + "\n");
				} else {
					sw2.append(spaces + line.replaceAll("^ +", "") + "\n");
				}
				
			}
			sw2.append(str);
//			System.out.println(sw2.toString());
			
			return sw2.toString();

		} else {
			return null;
		}
	}

}
