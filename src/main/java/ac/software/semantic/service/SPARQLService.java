package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.EmbedderIndexElement;
import ac.software.semantic.model.index.IndexElementSelector;
import ac.software.semantic.model.index.PropertyIndexElement;
import ac.software.semantic.repository.EmbedderDocumentRepository;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.ASVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;

@Service
public class SPARQLService {

	@Autowired
	private SEMRVocabulary resourceVocabulary;

	@Autowired
	private EmbedderDocumentRepository embedderRepository;
	
	public String generatorFilter(String var, List<String> generatorUris) {
		String filter = "";
		
		for (String uri : generatorUris) {
			filter += "<" + uri + "> ";
		}
	
		if (filter.length() > 0) {
			filter = "?" + var + " <" + ASVocabulary.generator + "> ?generator . VALUES ?generator { " + filter + " } . ";
		}
		
		return filter;
	}	
	
	private static String prefix = "http://app-semantic-backend.net/temp/";
	
	public SPARQLStructure toSPARQL(ClassIndexElement cie) {
		List<List<Resource>> res = new ArrayList<>();
		List<Resource> start = new ArrayList<>();
		
		Set<String> keys = new TreeSet<>();
		Map<String, String> targets = new TreeMap<>();
		
		String s = toSPARQL(cie, 0, start, res, keys, targets);
		
		return new SPARQLStructure(s, keys, targets);
	}
	
	public class SPARQLStructure {
		private String whereClause;
		
		private Set<String> keys;
		private Map<String, String> targets;
		
		public SPARQLStructure(String whereClause, Set<String> keys, Map<String, String> targets) {
			this.whereClause = whereClause;
			this.keys = keys;
			this.targets = targets;
		}

		public String getWhereClause() {
			return whereClause;
		}

		public void setWhereClause(String whereClause) {
			this.whereClause = whereClause;
		}

//		public List<List<Resource>> getPaths() {
//			return paths;
//		}
//
//		public void setPaths(List<List<Resource>> paths) {
//			this.paths = paths;
//		}
//		
//		public int size() {
//			return paths.size();
//		}

		public List<RDFNode> getResultForKey(Model model, Resource top, String key) {
			List<RDFNode> res = new ArrayList<>();
			
        	StmtIterator stmtIter = model.listStatements(top, model.createProperty(prefix + key), (RDFNode)null);
        	while (stmtIter.hasNext()) {
        		Statement stmt = stmtIter.next();
        		
        		res.add(stmt.getObject());
        	}
        	
        	return res;
		}
		
		public String construct(String fromClause) {
			StringBuffer sb = new StringBuffer();
			sb.append("CONSTRUCT { ");
			for (String key : keys) {
				sb.append("?c0 <" + prefix + key + "> ?" + key + " . ");
			}
			sb.append("} " + fromClause + " WHERE { " + whereClause + " } ");
				
			return sb.toString();
		}
		
		public String construct(String fromClause, Resource top) {
			StringBuffer sb = new StringBuffer();
			sb.append("CONSTRUCT { ");
			for (String key : keys) {
				sb.append("?c0 <" + prefix + key + "> ?" + key + " . ");
			}
			String stop = "<" + top + "> ";
			sb.append("} " + fromClause + " WHERE { VALUES ?c0  { " + stop + " } " + whereClause + " } ");
				
			return sb.toString();
		}
		
		public String construct(String fromClause, String top) {
			
			StringBuffer sb = new StringBuffer();
			sb.append("CONSTRUCT { ");
			for (String key : keys) {
				
				if (targets != null && targets.get(key) != null ) {
					String prop = targets.get(key);
					if (prop.startsWith("http://") || prop.startsWith("https://")) {
						sb.append("?c0 <" + prop  + "> ?" + key + " . ");
					} else {
						sb.append("?c0 <" + prefix + prop  + "> ?" + key + " . ");
					}
				} else {
					sb.append("?c0 <" + prefix + key + "> ?" + key + " . ");
				}
			}
			String stop = "<" + top + "> ";
			sb.append("} " + fromClause + " WHERE { VALUES ?c0  { " + stop + " } " + whereClause + " } ");
				
			return sb.toString();
		}
		
		public String construct(String fromClause, List<Resource> top) {
			StringBuffer sb = new StringBuffer();
			sb.append("CONSTRUCT { ");
			for (String key : keys) {
				sb.append("?c0 <" + prefix + key + "> ?" + key + " . ");
			}
			String stop = "";
			for (Resource t : top) {
				stop += "<" + t + "> ";
			}
			sb.append("} " + fromClause + " WHERE { VALUES ?c0  { " + stop + " } " + whereClause + " } ");
				
			return sb.toString();
		}

		public Set<String> getKeys() {
			return keys;
		}

		public Map<String, String> getTargets() {
			return targets;
		}

		public void setTargets(Map<String, String> targets) {
			this.targets = targets;
		}

//		public void setKeys(Set<String> keys) {
//			this.keys = keys;
//		}
	}
	
	private String toSPARQL(ClassIndexElement cie, int i, List<Resource> path, List<List<Resource>> res, Set<String> keys, Map<String, String> targets) {
		StringBuffer sb = new StringBuffer();
//		if (clazz.isURIResource()) {
		if (cie.getClazz() != null && !cie.getClazz().startsWith("_:")) {
			sb.append("?c" + i + " a <" + cie.getClazz().toString() + "> . ");
			path.add(new ResourceImpl(cie.getClazz()));
		}
		
		if (cie.getProperties() != null) {
			for (PropertyIndexElement nie : cie.getProperties()) {
				String prop = nie.getProperty();
				ClassIndexElement ie = nie.getElement();
				
				for (IndexElementSelector ies : nie.getSelectors()) {
					List<String> languages = ies.getLanguages();
		
					List<Resource> newPath = new ArrayList<>();
					newPath.addAll(path);
					newPath.add(new PropertyImpl(prop));
		
					if (ie == null) {
						String var = "?r" + ies.getIndex();
						String filter = "";
						if (languages != null) {
							for (int j = 0;  j < languages.size(); j++) {
								if (j > 0) {
									filter +=" || ";
								}
								filter += " langMatches(lang(" + var + "), \"" + languages.get(j) + "\" ) ";
							}
							if (filter.length() > 0) {
								filter = ". FILTER (" + filter + ")";
							}
						}
						sb.append("OPTIONAL { ?c" +  i + " <" + prop.toString() + "> " + var + filter + " } ");
						keys.add("r" + ies.getIndex());
						if (targets != null && ies.getTarget() != null) {
							targets.put("r" + ies.getIndex(), ies.getTarget());
						}
		//				res.add(newPath);
					} else {
						sb.append("OPTIONAL { ?c" +  i + " <" + prop.toString() + "> ?c" + (i + 1) + " . " + toSPARQL(ie, i + 1, newPath, res, keys, targets) + "} ");
					}
				}
			}
		}
		
		if (cie.getEmbedders() != null) {
			for (EmbedderIndexElement eie : cie.getEmbedders()) {
				Optional<EmbedderDocument> edocOpt = embedderRepository.findById(eie.getEmbedderId());
				
				if (!edocOpt.isPresent()) {
					continue;
				}
				
				EmbedderDocument edoc = edocOpt.get();
				
				String generatorFilter = generatorFilter("v", Arrays.asList(new String[] {resourceVocabulary.getEmbedderAsResource(edoc.getUuid()).toString()}));

//		        
				for (IndexElementSelector ies : eie.getSelectors()) {
					String var = "?r" + ies.getIndex();
					
					String embedding = 
							" ?v a <" + SEMAVocabulary.EmbeddingAnnotation + "> ; " + 
							"   <" + OAVocabulary.hasTarget + "> ?target ; " + 
							"   <" + OAVocabulary.hasBody + "> " + var + " . " +
							generatorFilter + 
							" ?target <" + OAVocabulary.hasSource + "> ?c" + i + " . ";
					
					sb.append("OPTIONAL { " + embedding + " } ");
					
					keys.add("r" + ies.getIndex());
					
					if (targets != null && ies.getTarget() != null) {
						targets.put("r" + ies.getIndex(), ies.getTarget());
					}

				}
			}
			
		}

		
//		if (i == 0) {
//			sb.append("SELECT * FROM <" + graph + "> WHERE { ");
//			sb.append("}");
//		}
		
		return sb.toString();
	}
	
}
