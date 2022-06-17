package ac.software.semantic.service;

import static ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest.ANNOTATED_ONLY;
import static ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest.UNANNOTATED_ONLY;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import ac.software.semantic.payload.UpdateAnnotatorRequest;
import ac.software.semantic.payload.ValueAnnotation;
import ac.software.semantic.payload.ValueAnnotationDetail;

import org.apache.derby.vti.Restriction;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;

import ac.software.semantic.controller.AsyncUtils;
import ac.software.semantic.controller.ExecuteMonitor;
import ac.software.semantic.controller.SSEController;
import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;
import ac.software.semantic.model.AnnotationEdit;
//import ac.software.semantic.model.AnnotationDocument;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.AnnotationEditType;
import ac.software.semantic.model.AnnotationEditValue;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.DataService;
import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.DataServiceParameterValue;
import ac.software.semantic.model.DataServiceVariant;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.ExecuteNotificationObject;
import ac.software.semantic.model.ExecuteState;
import ac.software.semantic.model.ExecutionInfo;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.MappingState;
import ac.software.semantic.model.NotificationObject;
import ac.software.semantic.model.PreprocessInstruction;
import ac.software.semantic.model.PublishState;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.payload.AnnotatorDocumentResponse;
import ac.software.semantic.repository.AnnotationEditGroupRepository;
import ac.software.semantic.repository.AnnotationEditRepository;
//import ac.software.semantic.repository.AnnotationRepository;
import ac.software.semantic.repository.AnnotatorDocumentRepository;
import ac.software.semantic.repository.DataServiceRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.security.UserPrincipal;
import edu.ntua.isci.ac.common.db.rdf.RDFJenaConnection;
import edu.ntua.isci.ac.common.db.rdf.RDFSource;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.model.Utils;
import edu.ntua.isci.ac.d2rml.monitor.FileSystemOutputHandler;
import edu.ntua.isci.ac.d2rml.parser.Parser;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OWLTime;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SOAVocabulary;

@Service
public class AnnotatorService {

	Logger logger = LoggerFactory.getLogger(AnnotatorService.class);

	@Autowired
	private ModelMapper modelMapper;

	@Value("${annotation.validation.paged.page-size}")
	private int pageSize;
	
	@Value("${database.name}")
	private String database;

	@Value("${annotation.execution.folder}")
	private String annotationsFolder;

	@Value("${mapping.temp.folder}")
	private String tempFolder;

	@Autowired
	@Qualifier("virtuoso-configuration")
	private Map<String,VirtuosoConfiguration> virtuosoConfigurations;

	@Autowired
	@Qualifier("filesystem-configuration")
	private FileSystemConfiguration fileSystemConfiguration;

	@Value("${d2rml.execute.safe}")
	private boolean safeExecute;

	@Value("${d2rml.execute.shard-size}")
	private int shardSize;
	
	@Value("${dataservice.definition.folder}")
	private String dataserviceFolder;

	@Autowired
	AnnotatorDocumentRepository annotatorRepository;

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	AnnotationEditRepository annotationEditRepository;

	@Autowired
	AnnotationEditGroupRepository annotationEditGroupRepository;

	@Autowired
	DataServiceRepository dataServiceRepository;
	
	@Autowired
	VirtuosoJDBC virtuosoJDBC;

	public Optional<AnnotatorDocumentResponse> getAnnotator(UserPrincipal currentUser, String id) {

		Optional<AnnotatorDocument> doc = annotatorRepository.findByIdAndUserId(new ObjectId(id),
				new ObjectId(currentUser.getId()));
		if (doc.isPresent()) {
			AnnotatorDocument ad = doc.get();
			AnnotationEditGroup aeg = annotationEditGroupRepository
					.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(ad.getDatasetUuid(),
							ad.getOnProperty(), ad.getAsProperty(),
							new ObjectId(currentUser.getId()))
					.get();
			return Optional.of(modelMapper.annotator2AnnotatorResponse(virtuosoConfigurations.values(), ad, aeg));
		} else {
			return Optional.empty();
		}
	}

	public AnnotatorDocument createAnnotator(UserPrincipal currentUser, String datasetUri, List<String> onProperty,
			String asProperty, String annotator, String thesaurus, List<DataServiceParameterValue> parameters,
			List<PreprocessInstruction> preprocess, String variant) {

		String datasetUuid = SEMAVocabulary.getId(datasetUri);

		Optional<Dataset> ds = datasetRepository.findByUuid(datasetUuid);
		if (ds.isPresent()) {

			String uuid = UUID.randomUUID().toString();

			AnnotatorDocument d = new AnnotatorDocument();
			d.setUserId(new ObjectId(currentUser.getId()));
			d.setDatasetUuid(datasetUuid);
			d.setUuid(uuid);
			d.setOnProperty(onProperty);
			d.setAsProperty(asProperty);
			d.setAnnotator(annotator);
			if (parameters != null) {
				d.setParameters(parameters);
			}
			if (thesaurus != null) {
				d.setThesaurus(thesaurus);
			}
			if (preprocess != null) {
				d.setPreprocess(preprocess);
			}
			if (variant != null) {
				d.setVariant(variant);
			}

			AnnotationEditGroup aeg;
			Optional<AnnotationEditGroup> aegOpt = annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(datasetUuid, onProperty, asProperty, new ObjectId(currentUser.getId()));
			if (!aegOpt.isPresent()) {
//				aeg = new AnnotationEditGroup(datasetUuid, onProperty, asProperty, new ObjectId(currentUser.getId()));
				aeg = new AnnotationEditGroup();
				aeg.setDatasetUuid(datasetUuid);
				aeg.setOnProperty(onProperty);
				aeg.setAsProperty(asProperty);
				aeg.setUserId(new ObjectId(currentUser.getId()));
				aeg.setUuid(UUID.randomUUID().toString());
				annotationEditGroupRepository.save(aeg);
			} else {
				aeg = aegOpt.get();
			}
			d.setAnnotatorEditGroupId(aeg.getId());

			AnnotatorDocument doc = annotatorRepository.save(d);

			return doc;
		} else {
			return null;
		}
	}

	//	@Autowired
//	private Environment env;
//
//	private static String currentTime() {
//		SimpleDateFormat srcFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
//		return srcFormatter.format(new Date());
//	}
	public AnnotatorDocument updateAnnotator(UserPrincipal currentUser, String annotatorId, UpdateAnnotatorRequest updateAnnotatorRequest) {
		Optional<AnnotatorDocument> annotatorOpt = annotatorRepository.findByIdAndUserId(new ObjectId(annotatorId), new ObjectId(currentUser.getId()));
		if (!annotatorOpt.isPresent()) {
			return null;
		}

		AnnotatorDocument annotator = annotatorOpt.get();
		if (updateAnnotatorRequest.getAnnotator() != null) {
			annotator.setAnnotator(updateAnnotatorRequest.getAnnotator());
		}
		if (updateAnnotatorRequest.getAsProperty() != null) {
			annotator.setAsProperty(updateAnnotatorRequest.getAsProperty());
		}
		if (updateAnnotatorRequest.getParameters() != null) {
			annotator.setParameters(updateAnnotatorRequest.getParameters());
		}
		if (updateAnnotatorRequest.getThesaurus() != null) {
			annotator.setThesaurus(updateAnnotatorRequest.getThesaurus());
		}
		if (updateAnnotatorRequest.getPreprocess() != null) {
			annotator.setPreprocess(updateAnnotatorRequest.getPreprocess());
		}
		if (updateAnnotatorRequest.getVariant() != null) {
			annotator.setVariant(updateAnnotatorRequest.getVariant());
		}

		ExecuteState es = annotator.getExecuteState(fileSystemConfiguration.getId());

		// Clearing old files
		if (es.getExecuteState() == MappingState.EXECUTED) {
			for (int i = 0; i < es.getExecuteShards(); i++) {
				(new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + annotator.getUuid()
						+ (i == 0 ? "" : "_#" + i) + ".trig")).delete();
			}
			new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + annotator.getUuid()
					+ "_catalog.trig").delete();
			new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + annotator.getUuid() + ".zip").delete();
		}

		annotator.setExecute(null);
		AnnotatorDocument res = annotatorRepository.save(annotator);
		return res;
	}

	public Optional<String> getLastExecution(UserPrincipal currentUser, String id) throws IOException {

//      List<MappingExecution> docs = executionsRepository.findLatestByUserIdAndD2rmlId(new ObjectId(currentUser.getId()), new ObjectId(mappingId));
		Optional<AnnotatorDocument> entry = annotatorRepository.findByIdAndUserId(new ObjectId(id),
				new ObjectId(currentUser.getId()));

		if (entry.isPresent()) {
			AnnotatorDocument doc = entry.get();

			Path path = Paths.get(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder
					+ doc.getUuid() + ".trig");
//	      	String file = new String(Files.readAllBytes(path));
			String file = AsyncUtils.readFileBeginning(path);

			return Optional.of(file);
		} else {
			return Optional.empty();
		}
	}

	public Optional<String> downloadLastExecution(UserPrincipal currentUser, String id) throws IOException {

		Optional<AnnotatorDocument> entry = annotatorRepository.findByIdAndUserId(new ObjectId(id),
				new ObjectId(currentUser.getId()));

		if (entry.isPresent()) {
			AnnotatorDocument doc = entry.get();

			ExecuteState  es = doc.getExecuteState(fileSystemConfiguration.getId());
			
			String file = fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + doc.getUuid() + ".zip";

			if (es.getExecuteState() == MappingState.EXECUTED) {
				if (!new File(file).exists()) {
					zipExecution(currentUser, doc, es.getExecuteShards() == 0 ? 1 : es.getExecuteShards()); //compatibility
				}
				return Optional.of(file);
			}
			
		} 
			
		return Optional.empty();
		

	}

	@Autowired
	ResourceLoader resourceLoader;

	private String applyPreprocessToMappingDocument(AnnotatorDocument adoc, Map<String, Object> params) throws Exception {
		
		Optional<DataService> dsOpt = dataServiceRepository.findByIdentifierAndType(adoc.getAnnotator(), DataServiceType.ANNOTATOR);
		if (!dsOpt.isPresent()) {
			throw new Exception("Annotator " + adoc.getAnnotator() + " not found");
		}
		
		DataService ds = dsOpt.get(); 

		// put default values if not exist
		for (DataServiceParameter dsp : ds.getParameters()) {
			if (!params.containsKey(dsp.getName()) && dsp.getDefaultValue() != null) {
				params.put(dsp.getName(), dsp.getDefaultValue());
			}
		}
		
		String variant = adoc.getVariant();
		List<DataServiceVariant> variants = ds.getVariants();

		String d2rmlPath = null;
		if (variant == null) {
			d2rmlPath = variants.get(0).getD2rml();
		} else {
			for (int i = 0; i < variants.size(); i++) {
				if (variants.get(i).getName().equals(adoc.getVariant())) {
					d2rmlPath = variants.get(i).getD2rml();
					break;
				}
			}
		}
		
		d2rmlPath = dataserviceFolder + d2rmlPath; 

		try (InputStream inputStream = resourceLoader.getResource("classpath:" + d2rmlPath).getInputStream()) {
			String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);

			StringBuffer preprocess = new StringBuffer();
			List<PreprocessInstruction> pis = adoc.getPreprocess();
			if (pis != null) {
				preprocess.append("dr:definedColumns ( \n");
				for (int i = 0; i < pis.size(); i++) {
					PreprocessInstruction pi = pis.get(i);
					
					preprocess.append("[ dr:name \"PREPROCESS-LEXICALVALUE-" + (i + 1) + "\" ; \n");
					preprocess.append("  dr:function <" + pi.getFunction() + "> ; \n");
					preprocess.append("dr:parameterBinding [ \n");
					preprocess.append("   dr:parameter \"input\" ; \n");
					if (i == 0) {
						preprocess.append("   rr:column \"lexicalValue\" ; \n");
					} else {
						preprocess.append("   rr:column \"PREPROCESS-LEXICALVALUE-" + i + "\" ; \n");
					}
					preprocess.append("] ; \n");
					for (DataServiceParameterValue pv : pi.getParameters()) {
						preprocess.append("dr:parameterBinding [ \n");
						preprocess.append("   dr:parameter \"" + pv.getName() + "\" ; \n");
						preprocess.append("   rr:constant \"" + pv.getValue() + "\" ; \n");
						preprocess.append("] ; \n");
					}
					preprocess.append("] \n");
				}
				
				preprocess.append("[ \n");
				preprocess.append("  dr:name \"PREPROCESS-LITERAL\" ; \n");
				preprocess.append("  dr:function <http://islab.ntua.gr/ns/d2rml-op#strlang> ; \n");
				preprocess.append("  dr:parameterBinding [ \n");
				preprocess.append("     dr:parameter \"lexicalValue\" ; \n");
				preprocess.append("     rr:column \"PREPROCESS-LEXICALVALUE-" + pis.size() + "\" ; \n");
				preprocess.append("  ] ; \n");
				preprocess.append("  dr:parameterBinding [ \n");
				preprocess.append("     dr:parameter \"language\" ; \n");
				preprocess.append("     rr:column \"language\" ; \n");
				preprocess.append("  ] ; \n");
				preprocess.append("] \n");
				
				preprocess.append(") ; \n");
				
				str = str.replace("{##ppPREPROCESS##}", preprocess.toString());
				str = str.replace("{##ppLITERAL##}", "PREPROCESS-LITERAL");
				str = str.replace("{##ppLEXICAL-VALUE##}", "PREPROCESS-LEXICALVALUE-" + pis.size());
			
			} else {
				str = str.replace("{##ppPREPROCESS##}", "");
				str = str.replace("{##ppLITERAL##}", "literal");
				str = str.replace("{##ppLEXICAL-VALUE##}", "lexicalValue");
			}

			return str;
		}

	}
	
	public boolean executeAnnotator(UserPrincipal currentUser, String id, ApplicationEventPublisher applicationEventPublisher) {

		Optional<AnnotatorDocument> res = annotatorRepository.findByIdAndUserId(new ObjectId(id),
				new ObjectId(currentUser.getId()));

		if (!res.isPresent()) {
			logger.info("Annotator " + id + " not found"); 
			return false;
		}

		AnnotatorDocument adoc = res.get();

		ExecuteState es = adoc.getExecuteState(fileSystemConfiguration.getId());

		// Clearing old files
		if (es.getExecuteState() == MappingState.EXECUTED) {
			for (int i = 0; i < es.getExecuteShards(); i++) {
				(new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid()
						+ (i == 0 ? "" : "_#" + i) + ".trig")).delete();
			}
			new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid()
					+ "_catalog.trig").delete();
			new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid() + ".zip").delete();
		}

		Date executeStart = new Date(System.currentTimeMillis());

		es.setExecuteState(MappingState.EXECUTING);
		es.setExecuteStartedAt(executeStart);
		es.setExecuteShards(0);
		es.setCount(0);

		annotatorRepository.save(adoc);

		logger.info("Annotator " + id + " starting");
		
		try (FileSystemOutputHandler outhandler = new FileSystemOutputHandler(
				fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder, adoc.getUuid(),
				shardSize)) {

			Optional<Dataset> dres = datasetRepository.findByUuid(adoc.getDatasetUuid());

			if (!dres.isPresent()) {
				return false;
			}

			Dataset ds = dres.get();
			

			String prop = "";
			for (int i = adoc.getOnProperty().size() - 1; i >= 0; i--) {
				if (i < adoc.getOnProperty().size() - 1) {
					prop += "/";
				}
				prop += "<" + adoc.getOnProperty().get(i) + ">";
			}

			Map<String, Object> params = new HashMap<>();

			VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
				
			params.put("iidatabase", vc.getSparqlEndpoint());
			params.put("iiproperty", prop);
			params.put("iigraph", SEMAVocabulary.getDataset(ds.getUuid()).toString());
//			params.put("iitime", currentTime());
			params.put("iirdfsource", vc.getSparqlEndpoint());
			params.put("iiannotator", SEMAVocabulary.getAnnotator(adoc.getUuid()));

			for (DataServiceParameterValue dsp : adoc.getParameters()) {
				params.put(dsp.getName(), dsp.getValue());
			}
			

			if (adoc.getThesaurus() != null) {
				
				logger.info("Annotator " + id + " reading thesaurus");
				
				String sparql = "SELECT ?url FROM <" + SEMAVocabulary.contentGraph + "> " + "WHERE { "
						+ " ?url <http://purl.org/dc/elements/1.1/identifier> <" + adoc.getThesaurus() + "> } ";

				try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(),
						QueryFactory.create(sparql, Syntax.syntaxARQ))) {
					ResultSet rs = qe.execSelect();
					while (rs.hasNext()) {
						QuerySolution sol = rs.next();

						params.put("iithesaurus", sol.get("url").toString());
						break;
					}
				}
			}

//			System.out.println(">> " + str);
//			System.out.println(">> " + params);

			logger.info("Annotator " + id + " preprocessing");
			
			String str = applyPreprocessToMappingDocument(adoc, params);

//			System.out.println(">> " + str);

			Executor exec = new Executor(outhandler, safeExecute);

			try (ExecuteMonitor em = new ExecuteMonitor("annotator", id, null, applicationEventPublisher)) {
				exec.setMonitor(em);

				D2RMLModel rmlMapping = D2RMLModel.readFromString(str);

				SSEController.send("annotator", applicationEventPublisher, this, new ExecuteNotificationObject(id, null,
						ExecutionInfo.createStructure(rmlMapping), executeStart));

				logger.info("Annotator started -- id: " + id);

				exec.keepSubjects(true);
				exec.execute(rmlMapping, params);

				Set<Resource> subjects = exec.getSubjects();

				try (Writer sw = new OutputStreamWriter(
						new FileOutputStream(new File(fileSystemConfiguration.getUserDataFolder(currentUser)
								+ annotationsFolder + adoc.getUuid() + "_catalog.trig"), false),
						StandardCharsets.UTF_8)) {
					sw.write("<" + SEMAVocabulary.getAnnotationSet(adoc.getUuid()).toString() + ">\n");
					sw.write("        <http://purl.org/dc/terms/hasPart>\n");
					sw.write("                ");
					int c = 0;
					for (Resource r : subjects) {
						if (c++ > 0) {
							sw.write(" , ");
						}
						sw.write("<" + r.getURI() + ">");
					}
					sw.write(" .");
				}

				Date executeFinish = new Date(System.currentTimeMillis());

				es.setExecuteCompletedAt(executeFinish);
				es.setExecuteState(MappingState.EXECUTED);
				es.setExecuteShards(outhandler.getShards());
//				es.setCount(outhandler.getTotalItems());
				es.setCount(subjects.size());

				annotatorRepository.save(adoc);

//	        	SSEController.send("annotator", applicationEventPublisher, this, new NotificationObject("execute", MappingState.EXECUTED.toString(), id, null, executeStart, executeFinish, outhandler.getTotalItems()));
				SSEController.send("annotator", applicationEventPublisher, this, new NotificationObject("execute", MappingState.EXECUTED.toString(), id, null, executeStart, executeFinish, subjects.size()));

				logger.info("Annotator executed -- id: " + id + ", shards: " + outhandler.getShards());

				try {
					zipExecution(currentUser, adoc, outhandler.getShards());
				} catch (Exception ex) {
					ex.printStackTrace();
					
					logger.info("Zipping annotator execution failed -- id: " + id);
				}
				
//				publishToDatabase(currentUser, new String[] {id});
//				
//				logger.info("Annotations copied to db -- id: " + id );
				
				return true;

			} catch (Exception ex) {
				ex.printStackTrace();
				
				logger.info("Annotator failed -- id: " + id);
				
				exec.getMonitor().currentConfigurationFailed();

				throw ex;
			}
		} catch (Exception ex) {
			ex.printStackTrace();

			es.setExecuteState(MappingState.EXECUTION_FAILED);

			SSEController.send("annotator", applicationEventPublisher, this,
					new NotificationObject("execute", MappingState.EXECUTION_FAILED.toString(), id, null, null, null));

			annotatorRepository.save(adoc);

			return false;
		}

	}

	private void zipExecution(UserPrincipal currentUser, AnnotatorDocument adoc, int shards) throws IOException {

		try (FileOutputStream fos = new FileOutputStream(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid() + ".zip");
				ZipOutputStream zipOut = new ZipOutputStream(fos)) {
			for (int i = 0; i < shards; i++) {
				File fileToZip = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid()
						+ (i == 0 ? "" : "_#" + i) + ".trig");

				try (FileInputStream fis = new FileInputStream(fileToZip)) {
					ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
					zipOut.putNextEntry(zipEntry);

					byte[] bytes = new byte[1024];
					int length;
					while ((length = fis.read(bytes)) >= 0) {
						zipOut.write(bytes, 0, length);
					}
				}
			}
		}
	}
	
//  problem with @prefix!!!	
//	private void zipExecution(UserPrincipal currentUser, AnnotatorDocument adoc, int shards) throws IOException {
//
//		try (FileOutputStream fos = new FileOutputStream(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid() + ".zip");
//				ZipOutputStream zipOut = new ZipOutputStream(fos)) {
//
//			File file = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid()
//			+ ".trig");
//
//			ZipEntry zipEntry = new ZipEntry(file.getName());
//			zipOut.putNextEntry(zipEntry);
//
//			for (int i = 0; i < shards; i++) {
//				File fileToZip = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid()
//					+ (i == 0 ? "" : "_#" + i) + ".trig");
//
//				try (FileInputStream fis = new FileInputStream(fileToZip)) {
//
//					byte[] bytes = new byte[1024];
//					int length;
//					while ((length = fis.read(bytes)) >= 0) {
//						zipOut.write(bytes, 0, length);
//					}
//				}
//			}
//		}
//	}	

	public List<AnnotatorDocumentResponse> getAnnotators(UserPrincipal currentUser, String datasetUri) {
		List<AnnotatorDocument> docs = new ArrayList<>();

		String datasetUuid = SEMAVocabulary.getId(datasetUri);

		docs = annotatorRepository.findByDatasetUuidAndUserId(datasetUuid, new ObjectId(currentUser.getId()));

		List<AnnotatorDocumentResponse> response = docs.stream()
				.map(doc -> modelMapper.annotator2AnnotatorResponse(virtuosoConfigurations.values(), doc,
						annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(
								doc.getDatasetUuid(), doc.getOnProperty(), doc.getAsProperty(),
								new ObjectId(currentUser.getId())).get()))
				.collect(Collectors.toList());

		return response;
	}
	
	public List<AnnotatorDocumentResponse> getAnnotators(String datasetUri) {
		List<AnnotatorDocument> docs = new ArrayList<>();

		String datasetUuid = SEMAVocabulary.getId(datasetUri);

		docs = annotatorRepository.findByDatasetUuid(datasetUuid);

		List<AnnotatorDocumentResponse> response = docs.stream()
				.map(doc -> modelMapper.annotator2AnnotatorResponse(virtuosoConfigurations.values(), doc,
						annotationEditGroupRepository.findByDatasetUuidAndOnPropertyAndAsProperty(
								doc.getDatasetUuid(), doc.getOnProperty(), doc.getAsProperty()).get()))
				.collect(Collectors.toList());
		return response;
	}

	public boolean publish(UserPrincipal currentUser, String[] ids) throws Exception {
		List<AnnotatorDocument> docs = new ArrayList<>();

		logger.info("PUBLICATION STARTED");
		
		VirtuosoConfiguration vc = null; // assumes all adocs belong to the same dataset and are published to the save virtuoso;
				
		for (String id : ids) {
			Optional<AnnotatorDocument> doc = annotatorRepository.findByIdAndUserId(new ObjectId(id),
					new ObjectId(currentUser.getId()));

			if (doc.isPresent()) {
				AnnotatorDocument adoc = doc.get();
				
				if (vc == null) {
					Optional<Dataset> dres = datasetRepository.findByUuid(adoc.getDatasetUuid());
					Dataset ds = dres.get();
					vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
				}

				PublishState state = adoc.getPublishState(vc.getId());
				state.setPublishState(DatasetState.PUBLISHING);
				state.setPublishStartedAt(new Date(System.currentTimeMillis()));

				annotatorRepository.save(adoc);

				docs.add(adoc);
			}
		}

//		logger.info("PUBLICATING TO VIRTUOSO");
		
		virtuosoJDBC.publish(currentUser, vc.getName(), docs);

		for (AnnotatorDocument adoc : docs) {
			PublishState state = adoc.getPublishState(vc.getId());
			state.setPublishCompletedAt(new Date(System.currentTimeMillis()));
			state.setPublishState(DatasetState.PUBLISHED);

			annotatorRepository.save(adoc);
		}

		System.out.println("PUBLICATION COMPLETED");

		return true;
	}

	public boolean unpublish(UserPrincipal currentUser, String[] ids) throws Exception {
		List<AnnotatorDocument> docs = new ArrayList<>();

		logger.info("UNPUBLICATION STARTED");

		VirtuosoConfiguration vc = null; // assumes all adocs belong to the same dataset and are published to the save virtuoso;
		
		for (String id : ids) {
			Optional<AnnotatorDocument> doc = annotatorRepository.findByIdAndUserId(new ObjectId(id),
					new ObjectId(currentUser.getId()));

			if (doc.isPresent()) {
				AnnotatorDocument adoc = doc.get();

				if (vc == null) {
					Optional<Dataset> dres = datasetRepository.findByUuid(adoc.getDatasetUuid());
					Dataset ds = dres.get();
					vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
				}
				
				PublishState state = adoc.getPublishState(vc.getId());
				state.setPublishState(DatasetState.UNPUBLISHING);
				state.setPublishStartedAt(new Date(System.currentTimeMillis()));
				annotatorRepository.save(adoc);

				docs.add(adoc);
			}
		}
		
//		logger.info("UNPUBLICATING FROM VIRTUOSO");

		virtuosoJDBC.unpublish(vc.getName(), docs);

		for (AnnotatorDocument adoc : docs) {

			PublishState state = adoc.getPublishState(vc.getId());
			state.setPublishState(DatasetState.UNPUBLISHED);
			state.setPublishStartedAt(null);
			state.setPublishCompletedAt(null);

			annotatorRepository.save(adoc);
		}

		logger.info("UNPUBLICATION COMPLETED");

		return true;
	}
	
    private String getAnnotationsFolder(UserPrincipal currentUser) {
    	if (annotationsFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder.substring(0, annotationsFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder;
    	}    	
    }
    
//	public boolean publishToDatabase(UserPrincipal currentUser, String[] ids) throws Exception {
//		List<AnnotatorDocument> docs = new ArrayList<>();
//
//		for (String id : ids) {
//			Optional<AnnotatorDocument> doc = annotatorRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//
//			if (doc.isPresent()) {
//				AnnotatorDocument adoc = doc.get();
//
////				PublishState state = adoc.getPublishState(virtuosoConfiguration.getId());
////				state.setPublishState(DatasetState.PUBLISHING);
////				state.setPublishStartedAt(new Date(System.currentTimeMillis()));
////
////				annotatorRepository.save(adoc);
//
//				docs.add(adoc);
//			}
//		}
//
////		virtuosoJDBC.publish(currentUser, docs);
//	    	
//	    String af = getAnnotationsFolder(currentUser);
//	    	
//		for (AnnotatorDocument adoc : docs) {
//			annotationRepository.deleteByGenerator(adoc.getUuid());
//
//			ExecuteState es = adoc.getExecuteState(fileSystemConfiguration.getId());
//		    		
//			for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
//				String fileName = adoc.getUuid() + (i == 0 ? "" : "_#" + i) + ".trig";
//
//				try (BufferedReader reader = new BufferedReader(new FileReader(af + File.separator + fileName))) {
//					StringBuffer annotation = new StringBuffer();
//
//					String line = reader.readLine();
//					while (line != null) {
////						System.out.println(line);
//						if (line.length() > 0) {
//							annotation.append("\n");
//							annotation.append(line);
//						}
//						
//						if (line.endsWith(".")) {
//							Model tmp = ModelFactory.createDefaultModel();
//							try (StringReader sr = new StringReader(annotation.toString())) {
//								tmp.read(sr, null, "ttl");
//							}
//							storeToMongo(currentUser, tmp);
//							annotation = new StringBuffer();
//						}
//						line = reader.readLine();
//					}
//				}
//	    	}
//
//			return true;
//	    }    
//
//		System.out.println("PUBLICATION COMPLETED");
//
//		return true;
//	}
	
	@Autowired
	@Qualifier("date-format")
	private SimpleDateFormat dateFormat;

	
//	private void storeToMongo(UserPrincipal currentUser, Model model) {
//		AnnotationDocument ann = new AnnotationDocument();
//		
//		ann.setUserId(new ObjectId(currentUser.getId()));
//		
//		StmtIterator iter = model.listStatements(null, null, (RDFNode)null);
//		while (iter.hasNext()) {
//			Statement st = iter.next();
//			
//			if (st.getPredicate().equals(RDFVocabulary.type)) {
//				ann.setUuid(SEMAVocabulary.getId(st.getSubject().toString()));
//				ann.addType(st.getObject().toString());
//			} else if (st.getPredicate().equals(DCVocabulary.created)) {
//				try {
//					ann.setCreated(dateFormat.parse(st.getObject().toString().substring(0,19)));
//				} catch (ParseException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			} else if (st.getPredicate().equals(OAVocabulary.hasBody)) {
//				if (!st.getObject().isAnon()) {
//					ann.setHasBody(st.getObject().toString());
//				}
//			} else if (st.getPredicate().equals(OWLTime.intervalStartedBy) || st.getPredicate().equals(OWLTime.hasBeginning)) {
//				ann.setHasBodyStart(st.getObject().toString());
//			} else if (st.getPredicate().equals(OWLTime.intervalFinishedBy) || st.getPredicate().equals(OWLTime.hasEnd)) {
//				ann.setHasBodyEnd(st.getObject().toString());
//			} else if (st.getPredicate().equals(OAVocabulary.hasSource)) {
//				ann.setHasSource(st.getObject().toString());
//			} else if (st.getPredicate().equals(SOAVocabulary.start)) {
//				ann.setStart(st.getObject().asLiteral().getInt());
//			} else if (st.getPredicate().equals(SOAVocabulary.end)) {
//				ann.setEnd(st.getObject().asLiteral().getInt());
//			} else if (st.getPredicate().equals(SOAVocabulary.score)) {
//				ann.setScore(st.getObject().asLiteral().getDouble());
//			} else if (st.getPredicate().equals(SOAVocabulary.onProperty)) {
//				ann.setOnProperty(st.getObject().toString());
//			} else if (st.getPredicate().equals(SOAVocabulary.onValue)) {
//				ann.setOnValue(st.getObject().asLiteral().getLexicalForm());
//				String lang = st.getObject().asLiteral().getLanguage();
//				if (lang.length() > 0) {
//					ann.setOnValueLanguage(st.getObject().asLiteral().getLanguage());				
//				}
//			} else if (st.getPredicate().equals(ASVocabulary.generator)) {
//				ann.setGenerator(SEMAVocabulary.getId(st.getObject().toString()));
//			}
//		}
//		
//		annotationRepository.save(ann);
//
//
//	}

	public boolean deleteAnnotator(UserPrincipal currentUser, String id) {
		Optional<AnnotatorDocument> doc = annotatorRepository.findByIdAndUserId(new ObjectId(id),
				new ObjectId(currentUser.getId()));

		if (doc.isPresent()) {
			AnnotatorDocument adoc = doc.get();
			
			ExecuteState es = adoc.getExecuteState(fileSystemConfiguration.getId());

			for (int i = 0; i < es.getExecuteShards(); i++) {
				try {
					(new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid()
							 + (i == 0 ? "" : "_#" + i) + ".trig")).delete();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			try {
				new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid()
						+ "_catalog.trig").delete();
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				new File(fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid()
						+ ".zip").delete();
			} catch (Exception e) {
				e.printStackTrace();
			}

			annotatorRepository.delete(adoc);

			if (annotatorRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(adoc.getDatasetUuid(),
					adoc.getOnProperty().toArray(new String[] {}), adoc.getAsProperty(),
					new ObjectId(currentUser.getId())).isEmpty()) {
				annotationEditRepository.deleteByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(adoc.getDatasetUuid(),
						adoc.getOnProperty(), adoc.getAsProperty(), new ObjectId(currentUser.getId()));
				annotationEditGroupRepository.deleteByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(
						adoc.getDatasetUuid(), adoc.getOnProperty().toArray(new String[] {}), adoc.getAsProperty(),
						new ObjectId(currentUser.getId()));
			}

			return true;

		}

		return false;

	}
	

	public org.apache.jena.query.Dataset load(UserPrincipal currentUser, AnnotatorDocument adoc) throws IOException {

		ExecuteState es = adoc.getExecuteState(fileSystemConfiguration.getId());
			
		org.apache.jena.query.Dataset ds = DatasetFactory.create();

		for (int i = 0; i < Math.min(1, es.getExecuteShards()); i++) { // load only 1st file ????
			String file = fileSystemConfiguration.getUserDataFolder(currentUser) + annotationsFolder + adoc.getUuid()
						+ (i == 0 ? "" : "_#" + i) + ".trig";

			RDFDataMgr.read(ds, "file:" + file, null, Lang.TRIG);
		}
		
		return ds;

	}
	
	public List<ValueCount> getValuesForPage(String onPropertyString, org.apache.jena.query.Dataset rdfDataset, int page) {
		
		// should also filter out URI values here but this would spoil pagination due to previous bug.
		String sparql = 
            "SELECT ?value ?valueCount WHERE { " +
			"  SELECT ?value (count(*) AS ?valueCount)" +
	        "  WHERE { " + 
            "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
		    "     <" + OAVocabulary.hasTarget + "> ?r . " + 
            "  ?r <" + SOAVocabulary.onProperty + "> \"" + onPropertyString + "\" ; " + 
		    "     <" + SOAVocabulary.onValue + "> ?value . "  + 
		    "    FILTER (isLiteral(?value)) " +		         
		    "  } " +
			"  GROUP BY ?value " + 
			"  ORDER BY desc(?valueCount) ?value } " + 
 		    "LIMIT " + pageSize + " OFFSET " + pageSize * (page - 1);


    	List<ValueCount> values = new ArrayList<>();
//	    System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
    	
    	try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), rdfDataset)) {
    		
    		ResultSet rs = qe.execSelect();
    		
    		while (rs.hasNext()) {
    			QuerySolution qs = rs.next();
    			RDFNode value = qs.get("value");
    			int count = qs.get("valueCount").asLiteral().getInt(); //valueCount is the number a value appears (not of annotations on value)
    			
    			values.add(new ValueCount(value, count));
    		}
    	}
    	
    	return values;
		
	}
	
	public Collection<ValueAnnotation> view(UserPrincipal currentUser, AnnotatorDocument adoc, org.apache.jena.query.Dataset rdfDataset, int page) {
		
    	String spath = AnnotationEditGroup.onPropertyListAsString(adoc.getOnProperty());
    	
    	List<ValueCount> values = getValuesForPage(spath, rdfDataset, page);
    	
		Map<AnnotationEditValue, ValueAnnotation> res = new LinkedHashMap<>();

    	StringBuffer sb = new StringBuffer();
    	for (ValueCount vc : values) {
			AnnotationEditValue aev = null;
    		
    		if (vc.getValue().isLiteral()) {
				Literal l = vc.getValue().asLiteral();
				String lf = l.getLexicalForm();
				
				lf = Utils.escapeLiteralNoDoubleQuotes(lf);
				sb.append(NodeFactory.createLiteralByValue(lf, l.getLanguage(), l.getDatatype()).toString());
	    		sb.append(" ");
				
				aev = new AnnotationEditValue(vc.getValue().asLiteral());
			} else {
				//ignore URI values. They should not be returned by getValuesForPage 
				
//				sb.append("<" + vc.getValue().toString() + ">");
//	    		sb.append(" ");

//				aev = new AnnotationEditValue(vc.getValue().asResource());
			}
    		
    		if (aev != null) {
				ValueAnnotation va = new ValueAnnotation();
				va.setOnValue(aev);
				va.setCount(vc.getCount()); // the number of appearances of the value
				
				res.put(aev, va);
    		}
    	}

    	String valueString = sb.toString();
    	
//    	System.out.println(valueString);


		String sparql = 
					"SELECT distinct ?value ?t ?ie ?start ?end  (count(*) AS ?count)" + 
			        "WHERE { " +  
			        " ?v <" + OAVocabulary.hasTarget + "> ?r . " + 
				    " { ?v <" + OAVocabulary.hasBody + "> ?t . FILTER (!isBlank(?t)) } UNION " + 
				    " { ?v <" + OAVocabulary.hasBody + "> [ " + 
				    " a <" + OWLTime.DateTimeInterval + "> ; " + 
				    " <" + OWLTime.intervalStartedBy + ">|<" + OWLTime.hasBeginning + "> ?t ; " + 
				    " <" + OWLTime.intervalFinishedBy + ">|<" + OWLTime.hasEnd + "> ?ie ]  }  " +
				    "  ?r <" + SOAVocabulary.onProperty + "> \"" + spath + "\" ; " + 
				    "     <" + SOAVocabulary.onValue + "> ?value  . " + 
				    "  VALUES ?value { " + valueString  + " } " +
				    " OPTIONAL { ?r <" + SOAVocabulary.start + "> ?start } . " + 
				    " OPTIONAL { ?r <" + SOAVocabulary.end + "> ?end } . " + " } " +
		            "GROUP BY ?t ?ie ?value ?start ?end " +
					"ORDER BY DESC(?count) ?value ?start ?end";
//    	System.out.println(sparql);
    	
//    	System.out.println(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11));
    	
		if (valueString.length() > 0) {
			try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxSPARQL_11), rdfDataset)) {
				ResultSet rs = qe.execSelect();
				
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					
					RDFNode value = sol.get("value");
					
					String ann = sol.get("t") != null ? sol.get("t").toString() : null;
					String ie = sol.get("ie") != null ? sol.get("ie").toString() : null;
					int start = sol.get("start") != null ? sol.get("start").asLiteral().getInt() : -1;
					int end = sol.get("end") != null ? sol.get("end").asLiteral().getInt() : -1;
					int count = sol.get("count").asLiteral().getInt();
					
					AnnotationEditValue aev = null;
					if (value.isResource()) {
						aev = new AnnotationEditValue(value.asResource());
					} else if (value.isLiteral()) {
						aev = new AnnotationEditValue(value.asLiteral());
					}
					
					
					ValueAnnotation va = res.get(aev);
					
					if (va != null && ann != null) {
						ValueAnnotationDetail vad = new ValueAnnotationDetail();
						vad.setValue(ann);
						vad.setValue2(ie);
						vad.setStart(start);
						vad.setEnd(end);
						vad.setCount(count); // the number of appearances of the annotation 
						                     // it is different than the number of appearances of the value if multiple annotations exist on the same value
						
						va.getDetails().add(vad);
					} 
				}

			}
		}
		
		return res.values();
    } 
}
