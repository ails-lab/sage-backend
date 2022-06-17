package ac.software.semantic.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;

import ac.software.semantic.controller.ExecuteMonitor;
import ac.software.semantic.controller.SSEController;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.DataServiceParameterValue;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.ExecuteNotificationObject;
import ac.software.semantic.model.ExecuteState;
import ac.software.semantic.model.ExecutionInfo;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.IndexState;
import ac.software.semantic.model.IndexingState;
import ac.software.semantic.model.MappingState;
import ac.software.semantic.model.NotificationObject;
import ac.software.semantic.model.PreprocessInstruction;
import ac.software.semantic.model.PublishState;
import ac.software.semantic.model.VirtuosoConfiguration;
import ac.software.semantic.model.VocabularizerDocument;
import ac.software.semantic.payload.VocabularizerResponse;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.VocabularizerRepository;
import ac.software.semantic.security.UserPrincipal;
import edu.ntua.isci.ac.common.db.rdf.RDFJenaConnection;
import edu.ntua.isci.ac.common.db.rdf.RDFJenaSource;
import edu.ntua.isci.ac.common.db.rdf.RDFSource;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.monitor.FileSystemOutputHandler;
import edu.ntua.isci.ac.d2rml.monitor.StringOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.SKOSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import edu.ntua.isci.ac.semaspace.index.Indexer;

@Service
public class VocabularizerService {

	@Autowired
	private ModelMapper modelMapper;

	@Value("${vocabularizer.execution.folder}")
	private String vocabularizersFolder;

	@Value("${vocabularizer.header.d2rml}")
	private String vocabularizerD2RML;

	@Autowired
	VocabularizerRepository vocabularizerRepository;

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	VirtuosoJDBC virtuosoJDBC;

	@Autowired
	@Qualifier("virtuoso-configuration")
	private Map<String,VirtuosoConfiguration> virtuosoConfigurations;

	@Autowired
	@Qualifier("elastic-configuration")
	private ElasticConfiguration elasticConfiguration;

	@Autowired
	@Qualifier("filesystem-configuration")
	private FileSystemConfiguration fileSystemConfiguration;

	@Value("${d2rml.execute.safe}")
	private boolean safeExecute;

	@Value("${d2rml.execute.shard-size}")
	private int shardSize;
	
    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;

	public Optional<VocabularizerResponse> getVocabularizer(UserPrincipal currentUser, String id) {

		Optional<VocabularizerDocument> doc = vocabularizerRepository.findByIdAndUserId(new ObjectId(id),
				new ObjectId(currentUser.getId()));
		if (doc.isPresent()) {
			VocabularizerDocument ad = doc.get();
			return Optional.of(modelMapper.vocabularizer2VocabularizerResponse(virtuosoConfigurations.values(), ad));
		} else {
			return Optional.empty();
		}
	}

	public VocabularizerDocument createVocabularizer(UserPrincipal currentUser, String datasetUri,
			List<String> onProperty, String name, String separator) {

		String datasetUuid = SEMAVocabulary.getId(datasetUri);

		Optional<Dataset> ds = datasetRepository.findByUuid(datasetUuid);
		if (ds.isPresent()) {

			String uuid = UUID.randomUUID().toString();

			VocabularizerDocument d = new VocabularizerDocument(new ObjectId(currentUser.getId()), datasetUuid, uuid,
					onProperty, name, separator);

			VocabularizerDocument doc = vocabularizerRepository.save(d);

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
//        return srcFormatter.format(new Date());
//	}
//	
	public Optional<String> getLastExecution(UserPrincipal currentUser, String id) throws IOException {

//      List<MappingExecution> docs = executionsRepository.findLatestByUserIdAndD2rmlId(new ObjectId(currentUser.getId()), new ObjectId(mappingId));
		Optional<VocabularizerDocument> entry = vocabularizerRepository.findByIdAndUserId(new ObjectId(id),
				new ObjectId(currentUser.getId()));

		if (entry.isPresent()) {
			VocabularizerDocument doc = entry.get();

			Path path = Paths.get(fileSystemConfiguration.getUserDataFolder(currentUser) + vocabularizersFolder
					+ doc.getUuid() + ".trig");
			String file = new String(Files.readAllBytes(path));

			return Optional.of(file);
		} else {
			return Optional.empty();
		}
	}

	@Autowired
	ResourceLoader resourceLoader;

	private String applyPreprocessToVocabularizerDocument(String d2rml, List<String> onProperty) throws Exception {

		try (InputStream inputStream = resourceLoader.getResource("classpath:" + d2rml).getInputStream()) {
			String str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);

			String props = "";
			for (int i = onProperty.size() - 1; i >= 0; i--) {
				props += "<" + onProperty.get(i) + ">";
//    			props[adoc.getOnProperty().size()-i-1] =  model.createResource(adoc.getOnProperty().get(i));
			}

			str = str.replace("{##ppPROPERTY_LIST##}", props);

			return str;
		}

	}

	public boolean executeVocabularizer(UserPrincipal currentUser, String id,
			ApplicationEventPublisher applicationEventPublisher) throws Exception {
//		
		Optional<VocabularizerDocument> res = vocabularizerRepository.findByIdAndUserId(new ObjectId(id),
				new ObjectId(currentUser.getId()));

		if (!res.isPresent()) {
			return false;
		}

		VocabularizerDocument adoc = res.get();

		Optional<Dataset> dres = datasetRepository.findByUuid(adoc.getDatasetUuid());

		if (!dres.isPresent()) {
			return false;
		}

		Dataset ds = dres.get();
	    VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

		Date executeStart = new Date(System.currentTimeMillis());

		ExecuteState es = adoc.getExecuteState(fileSystemConfiguration.getId());

		es.setExecuteState(MappingState.EXECUTING);
		es.setExecuteStartedAt(executeStart);
		es.setExecuteShards(0);
		es.setCount(0);

		vocabularizerRepository.save(adoc);

		List<String> onProperty = adoc.getOnProperty();

		String spath = "";
		for (int i = onProperty.size() - 1; i >= 0; i--) {
			if (i < onProperty.size() - 1) {
				spath += "/";
			}
			spath += "<" + onProperty.get(i) + ">";
		}

		String sparql = "SELECT DISTINCT ?value " + "WHERE { " + "  GRAPH <" + SEMAVocabulary.getDataset(ds.getUuid())
				+ "> { " + "    ?s " + spath + " ?value } } ";

		Set<String> values = new HashSet<>();

		try (FileSystemOutputHandler outhandler = new FileSystemOutputHandler(
				fileSystemConfiguration.getUserDataFolder(currentUser) + vocabularizersFolder, adoc.getUuid(),
				shardSize)) {

			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(),
					QueryFactory.create(sparql, Syntax.syntaxARQ))) {

				ResultSet rs = qe.execSelect();

				Model model = ModelFactory.createDefaultModel();

				// do it with d2rml
				if (adoc.getSeparator() != null && adoc.getSeparator().length() > 0) {
					while (rs.hasNext()) {
						QuerySolution sol = rs.next();

						String[] vv = sol.get("value").asLiteral().getLexicalForm().split(adoc.getSeparator());

						for (String v : vv) {
							v = v.trim();
							if (v.length() > 0) {

								if (values.add(v)) {
									Resource subj = SEMAVocabulary
											.getTerm(adoc.getUuid() + "#" + UUID.randomUUID().toString());

									try (StringOutputHandler conn = new StringOutputHandler()) {

										((RDFJenaConnection) conn.getConnection()).add(subj, RDFVocabulary.type,
												SKOSVocabulary.Concept);
										((RDFJenaConnection) conn.getConnection()).add(subj, SKOSVocabulary.prefLabel,
												model.createLiteral(v));

										outhandler.commitItem(conn);
									}
								}
							}
						}

					}
				} else {
					while (rs.hasNext()) {
						QuerySolution sol = rs.next();

						RDFNode v = sol.get("value");

						Resource subj = SEMAVocabulary.getTerm(adoc.getUuid() + "#" + UUID.randomUUID().toString());

						try (StringOutputHandler conn = new StringOutputHandler()) {

							((RDFJenaConnection) conn.getConnection()).add(subj, RDFVocabulary.type,
									SKOSVocabulary.Concept);
							((RDFJenaConnection) conn.getConnection()).add(subj, SKOSVocabulary.prefLabel, v);

							outhandler.commitItem(conn);
						}
					}
				}
			}

			outhandler.commit();

		} catch (Exception ex) {
			ex.printStackTrace();

			es.setExecuteState(MappingState.EXECUTION_FAILED);

			SSEController.send("vocabularizer", applicationEventPublisher, this,
					new NotificationObject("execute", MappingState.EXECUTION_FAILED.toString(), id, null, null, null));

			vocabularizerRepository.save(adoc);

			return false;
		}

		String str = applyPreprocessToVocabularizerDocument(vocabularizerD2RML, adoc.getOnProperty());

		Map<String, Object> params = new HashMap<>();
		params.put("id", SEMAVocabulary.getDataset(adoc.getUuid()).toString());
		params.put("identifier", SEMAVocabulary.getDataset(adoc.getUuid()).toString());
		params.put("name", adoc.getName());
		params.put("source", SEMAVocabulary.getDataset(adoc.getDatasetUuid()).toString());
		params.put("prefix", SEMAVocabulary.getTerm(adoc.getUuid()) + "#");
		if (adoc.getSeparator() != null && adoc.getSeparator().length() > 0) {
			params.put("separator", adoc.getSeparator());
		} else {
			params.put("separator", "");
		}

		try (FileSystemOutputHandler outhandler = new FileSystemOutputHandler(
				fileSystemConfiguration.getUserDataFolder(currentUser) + vocabularizersFolder,
				adoc.getUuid() + "_catalog", shardSize)) {

			Executor exec = new Executor(outhandler, safeExecute);

			try (ExecuteMonitor em = new ExecuteMonitor("vocabularizer", id, null, applicationEventPublisher)) {
				exec.setMonitor(em);

				D2RMLModel rmlMapping = D2RMLModel.readFromString(str);

				SSEController.send("vocabularizer", applicationEventPublisher, this, new ExecuteNotificationObject(id,
						null, ExecutionInfo.createStructure(rmlMapping), executeStart));

				exec.execute(rmlMapping, params);

				Date executeFinish = new Date(System.currentTimeMillis());

				es.setExecuteCompletedAt(executeFinish);
				es.setExecuteState(MappingState.EXECUTED);
				es.setExecuteShards(outhandler.getShards());
				es.setCount(outhandler.getTotalItems());

				vocabularizerRepository.save(adoc);

				SSEController.send("vocabularizer", applicationEventPublisher, this,
						new NotificationObject("execute", MappingState.EXECUTED.toString(), id, null, executeStart,
								executeFinish, outhandler.getTotalItems()));

				System.out.println("ANNOTATOR EXECUTED: " + outhandler.getShards());

				return true;
//			        RDFJenaConnection conn = (RDFJenaConnection)ts.getConnection();

//			        Model model = conn.getDataset().getDefaultModel();

				// D2RML DOES NOT SUPPORT LISTS
//					RDFNode[] props = new RDFNode[adoc.getOnProperty().size()]; 
//		    		for (int i = adoc.getOnProperty().size() -1; i >= 0; i--) {
//		    			props[adoc.getOnProperty().size()-i-1] =  model.createResource(adoc.getOnProperty().get(i));
//		    		}
//		    		conn.add(SEMAVocabulary.getDataset(adoc.getUuid()), SEMAVocabulary.onProperty, model.createList(props));

				// conn.saveAsTRIG(fileSystemConfiguration.getUserDataFolder(currentUser) +
				// vocabularizersFolder, adoc.getUuid() + "_catalog");

			} catch (Exception ex) {
				ex.printStackTrace();
				exec.getMonitor().currentConfigurationFailed();

				throw ex;
			}

		} catch (Exception ex) {
			ex.printStackTrace();

			es.setExecuteState(MappingState.EXECUTION_FAILED);

			SSEController.send("vocabularizer", applicationEventPublisher, this,
					new NotificationObject("execute", MappingState.EXECUTION_FAILED.toString(), id, null, null, null));

			vocabularizerRepository.save(adoc);

			return false;
		}
	}

//	
	public List<VocabularizerResponse> getVocabularizers(UserPrincipal currentUser, String datasetUri) {
		List<VocabularizerDocument> docs = new ArrayList<>();

		String datasetUuid = SEMAVocabulary.getId(datasetUri);

		docs = vocabularizerRepository.findByDatasetUuidAndUserId(datasetUuid, new ObjectId(currentUser.getId()));

		List<VocabularizerResponse> response = docs.stream()
				.map(doc -> modelMapper.vocabularizer2VocabularizerResponse(virtuosoConfigurations.values(), doc)).collect(Collectors.toList());

		return response;
	}

	public boolean publish(UserPrincipal currentUser, String id) throws Exception {
		Optional<VocabularizerDocument> doc = vocabularizerRepository.findByIdAndUserId(new ObjectId(id),
				new ObjectId(currentUser.getId()));

		if (doc.isPresent()) {
			VocabularizerDocument adoc = doc.get();
			
		    Dataset ds = datasetRepository.findByUuid(adoc.getDatasetUuid()).get();
		    VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

			PublishState state = adoc.getPublishState(vc.getId());
			state.setPublishState(DatasetState.PUBLISHING);
			state.setPublishStartedAt(new Date(System.currentTimeMillis()));

			vocabularizerRepository.save(adoc);

			virtuosoJDBC.publish(currentUser, vc.getName(), adoc);
			virtuosoJDBC.addDatasetToAccessGraph(currentUser, vc.getName(), adoc.getUuid(), false);

			state.setPublishCompletedAt(new Date(System.currentTimeMillis()));
			state.setPublishState(DatasetState.PUBLISHED);

			vocabularizerRepository.save(adoc);

			System.out.println("PUBLICATION COMPLETED");
		}

		return true;
	}

	public boolean unpublish(UserPrincipal currentUser, String id) throws Exception {
		Optional<VocabularizerDocument> doc = vocabularizerRepository.findByIdAndUserId(new ObjectId(id),
				new ObjectId(currentUser.getId()));

		if (doc.isPresent()) {
			VocabularizerDocument adoc = doc.get();
		    Dataset ds = datasetRepository.findByUuid(adoc.getDatasetUuid()).get();
		    VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

			PublishState state = adoc.getPublishState(vc.getId());
			state.setPublishState(DatasetState.UNPUBLISHING);
			state.setPublishStartedAt(new Date(System.currentTimeMillis()));
			vocabularizerRepository.save(adoc);

			virtuosoJDBC.unpublish(vc.getName(), adoc);
			virtuosoJDBC.removeDatasetFromAccessGraph(vc.getName(), adoc.getUuid().toString());

			state.setPublishState(DatasetState.UNPUBLISHED);
			state.setPublishStartedAt(null);
			state.setPublishCompletedAt(null);

			vocabularizerRepository.save(adoc);
		}

		System.out.println("UNPUBLICATION COMPLETED");

		return true;
	}

	public boolean index(UserPrincipal currentUser, String datasetId) throws IOException {
		Optional<VocabularizerDocument> doc = vocabularizerRepository.findByIdAndUserId(new ObjectId(datasetId),
				new ObjectId(currentUser.getId()));

		if (doc.isPresent()) {
			VocabularizerDocument vocabulary = doc.get();
		    Dataset ds = datasetRepository.findByUuid(vocabulary.getDatasetUuid()).get();
		    VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

			IndexState is = vocabulary.getIndexState(elasticConfiguration.getId());

			is.setIndexState(IndexingState.INDEXING);
			is.setIndexStartedAt(new Date(System.currentTimeMillis()));
			vocabularizerRepository.save(vocabulary);

			Indexer ic = new Indexer(vc.getSparqlEndpoint());
			ic.indexVocabulary(elasticConfiguration.getIndexIp(), elasticConfiguration.getIndexVocabularyName(),
					SEMAVocabulary.getDataset(vocabulary.getUuid()).toString(), legacyUris);

			System.out.println("INDEXING COMPLETED");

			is.setIndexState(IndexingState.INDEXED);
			is.setIndexCompletedAt(new Date(System.currentTimeMillis()));
			vocabularizerRepository.save(vocabulary);

			return true;
		}

		return false;

	}

	public boolean unindex(UserPrincipal currentUser, String datasetId) throws IOException {
		Optional<VocabularizerDocument> doc = vocabularizerRepository.findByIdAndUserId(new ObjectId(datasetId),
				new ObjectId(currentUser.getId()));

		if (doc.isPresent()) {
			VocabularizerDocument vocabulary = doc.get();
		    Dataset ds = datasetRepository.findByUuid(vocabulary.getDatasetUuid()).get();
		    VirtuosoConfiguration vc = ds.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());

			IndexState is = vocabulary.getIndexState(elasticConfiguration.getId());

			is.setIndexState(IndexingState.UNINDEXING);
			is.setIndexStartedAt(new Date(System.currentTimeMillis()));
			vocabularizerRepository.save(vocabulary);

			Indexer ic = new Indexer(vc.getSparqlEndpoint());
			ic.unindexVocabulary(elasticConfiguration.getIndexIp(), elasticConfiguration.getIndexVocabularyName(),
					SEMAVocabulary.getDataset(vocabulary.getUuid()).toString(), legacyUris);

			System.out.println("UNINDEXING COMPLETED");

			is.setIndexState(IndexingState.NOT_INDEXED);
			is.setIndexCompletedAt(new Date(System.currentTimeMillis()));
			vocabularizerRepository.save(vocabulary);

			return true;

		}

		return false;

	}

	public boolean deleteVocabularizer(UserPrincipal currentUser, String id) {
		Optional<VocabularizerDocument> doc = vocabularizerRepository.findByIdAndUserId(new ObjectId(id),
				new ObjectId(currentUser.getId()));

		if (doc.isPresent()) {
			VocabularizerDocument adoc = doc.get();

			try {
				new File(fileSystemConfiguration.getUserDataFolder(currentUser) + vocabularizersFolder + adoc.getUuid()
						+ ".trig").delete();
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				new File(fileSystemConfiguration.getUserDataFolder(currentUser) + vocabularizersFolder + adoc.getUuid()
						+ "_catalog.trig").delete();
			} catch (Exception e) {
				e.printStackTrace();
			}

			vocabularizerRepository.delete(adoc);

			return true;
		}

		return false;

	}
}
