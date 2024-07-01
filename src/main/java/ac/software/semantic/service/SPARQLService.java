package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
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
import ac.software.semantic.model.constants.type.RDFTermType;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.EmbedderIndexElement;
import ac.software.semantic.model.index.IndexElementSelector;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.model.index.PropertyIndexElement;
import ac.software.semantic.repository.core.EmbedderDocumentRepository;
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
	
	@Autowired
	private SparqlQueryService sparqlQueryService;
	
	public static String prefix = "http://app-semantic-backend.net/temp/";
	
	public SPARQLStructure toSPARQL(ClassIndexElement cie, Map<Integer,IndexKeyMetadata> ikm, boolean optional) {
		List<List<Resource>> res = new ArrayList<>();
		List<Resource> start = new ArrayList<>();
		
		Map<String, KeyData> keys = new TreeMap<>();  // key -> optional true/false
		Map<String, KeyData> nokeys = new TreeMap<>();  // keys only to bind values
		
		Map<String, String> targets = new TreeMap<>();
		Map<String, GroupStructure> groups = new HashMap<>();
		
		String s = toSPARQL(cie, ikm, "c", 0, start, res, keys, nokeys, targets, groups, null, optional);
		String st = null;
		if (!optional) {
			Set<String> filters = new HashSet<>();
			
			st = toTreeSPARQL(cie, ikm, start, res, keys, nokeys, targets, filters);
			for (String filter : filters) {
				st += filter + " . ";
			}

		}
		
		SPARQLStructure ss = new SPARQLStructure(s, st, keys, nokeys, targets, groups);
//		System.out.println("GB " + cie.getGroupBy());
		if (cie.getGroupBy() != null && cie.getGroupBy()) {
			ss.groupBy = true;
		}
		
		return ss;
	}
	
	private class GroupStructure {
		private String group;
		private String rootVariable;
		private List<String> terminalVariables;
		private GroupStructure parent;
		
		public GroupStructure(GroupStructure parent, String group, String rootVariable) {
			this.parent = parent;
			this.group = group;
			this.rootVariable = rootVariable;
			
			terminalVariables = new ArrayList<>();
		}
		
		public void addTerminalVariable(String terminalVariable) {
			terminalVariables.add(terminalVariable);
		}
		
		public String toString() {
			return this.hashCode() + ": " + group + " / " + rootVariable + " / " + terminalVariables + " / " + (parent != null ? parent.hashCode() : null);
		}
	}
	
	public class SPARQLStructure {
		private String whereClause;
		private String treeWhereClause;
		
		private Map<String, KeyData> keys;  // key -> optional yes/no
		private Map<String, KeyData> nokeys;  // key -> optional yes/no
		
		private Map<String, String> targets;
		private Map<String, GroupStructure> groups;
		
		private boolean groupBy;
		
		public SPARQLStructure(String whereClause, String treeWhereClause, Map<String, KeyData> keys, Map<String, KeyData> nokeys, Map<String, String> targets, Map<String, GroupStructure> groups) {
			this.whereClause = whereClause;
			this.treeWhereClause = treeWhereClause;
			this.keys = keys;
			this.nokeys = nokeys;
			this.targets = targets;
			this.groups = groups;
		}

		public String getWhereClause() {
			return whereClause;
		}
		
		public String getTreeWhereClause() {
			return treeWhereClause;
		}

		public void setWhereClause(String whereClause) {
			this.whereClause = whereClause;
		}
		
		public String getGroupByClause() {
			if (groupBy) {
				return " GROUP BY ?c_0 ";
			} else {
				return "";
			}
		}
		
		public boolean isGroupBy() {
			return groupBy;
		}
		
		public Map<String, List<String>> getKeyGroups() {
			Map<String, List<String>> res = new HashMap<>(); 
			
			List<String> used = new ArrayList<>();
			for (Map.Entry<String, GroupStructure> entry : groups.entrySet()) {
				res.put(entry.getKey(), entry.getValue().terminalVariables);
				used.addAll(entry.getValue().terminalVariables);
			}
			
			Set<String> k = new HashSet<>();
			k.addAll(keys.keySet());
			k.removeAll(used);
			
			res.put(null, new ArrayList<>(k));
			
			return res;
		}

		public List<RDFNode> getResultForKey(Model model, Resource top, String key) {
			List<RDFNode> res = new ArrayList<>();
			
        	StmtIterator stmtIter = model.listStatements(top, model.createProperty(prefix + key), (RDFNode)null);
        	while (stmtIter.hasNext()) {
        		Statement stmt = stmtIter.next();
        		
        		res.add(stmt.getObject());
        	}
        	
        	return res;
		}
		
		public List<RDFNode> getResultForKey(Model model, Resource top, List<String> path, List<String> pathElements, String key) {
			List<RDFNode> res = new ArrayList<>();

			String spath = "";
			String svars = "";
			if (spath != null) {
				for (int i = 0; i < path.size(); i++) {
					if (i > 0 ) {
						spath += "<" + pathElements.get(i - 1) + "> ";
					}
					spath += " <" + path.get(i) + "> <" + pathElements.get(i) + "> . ";
					if (i == path.size() - 1) {
						spath += "<" + pathElements.get(i) + ">";
					}
				}
			}
			
			String sparql = "SELECT " + svars + " ?v WHERE { <" + top + "> " + spath + " <" + prefix + key + "> ?v } ";
			try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql, Syntax.syntaxARQ), model)) {
				ResultSet rs = qe.execSelect();
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
        		
					res.add(sol.get("v"));
				}
        	}
        	
        	return res;
		}
		
		public String construct(String fromClause) {
			StringBuffer sb = new StringBuffer();
			sb.append("CONSTRUCT { ");
			for (String key : keys.keySet()) {
				sb.append("?c_0 <" + prefix + key + "> ?" + key + " . ");
			}
			sb.append("} " + fromClause + " WHERE { " + whereClause + " } ");
				
			return sb.toString();
		}

		public String whereClause() {
			return whereClause;
		}
		
		public String treeWhereClause() {
			return treeWhereClause;
		}
		
		public String filterClauses(boolean onlyLiteral, boolean lexicalValue, boolean language) {
			StringBuffer sb = new StringBuffer();
			for (Map.Entry<String, KeyData> keye : keys.entrySet()) {
				String key = keye.getKey();
				KeyData kd = keye.getValue();
				if (kd.values != null) {
					
				} else {
					if (onlyLiteral) {
						if (kd.optional) {
							sb.append("FILTER (isLiteral(?" + key + ") || !bound(?" + key + ")) . ");
						} else {
							sb.append("FILTER (isLiteral(?" + key + ")) . ");	
						}
					}
				}
				
				if (lexicalValue) {
					sb.append("BIND(STR(?" + key + ") AS ?lexicalValue_" + key + " ) . ");
				}
				if (language) {
					sb.append("BIND(LANG(?" + key + ") AS ?language_" + key + " ) . ");
					if (kd.values != null) {
						// because language_r is empty if VALUES ?r clause preceeds BIND.
//						sb.append("BIND(if(bound(?" + key + "), if(bound(?language_" + key + "_x),?language_" + key + "_x,\"\"), UNDEF) AS ?language_" + key + "). ");
					}
				}
			}
				
			return sb.toString();
		}
		
		public String valueClauses() {
			StringBuffer sb = new StringBuffer();
			for (Map.Entry<String, KeyData> keye : keys.entrySet()) {
				String key = keye.getKey();
				KeyData kd = keye.getValue();
				if (kd.values != null) {
					StringBuffer vsb =  new StringBuffer();
					vsb.append(" VALUES ?" + key + " { ");
					for (String s : kd.values) {
						vsb.append(s + " ");
					}
					vsb.append(" } ");
					
					sb.append("<#<{@@" + key + "@@}>> " + vsb.toString() + " <<{@@" + key + "@@}>> FILTER(!bound(?" + key + ")) <<{@@" + key + "@@}>#> . ");
				} else {
					sb.append("<#<{@@" + key + "@@}>> VALUES ?" + key + " { {@@" + key + "@@} } <<{@@" + key + "@@}>> FILTER(!bound(?" + key + ")) <<{@@" + key + "@@}>#> . ");
				}
			}
			for (Map.Entry<String, KeyData> keye : nokeys.entrySet()) {
				String key = keye.getKey();
				KeyData kd = keye.getValue();
				if (kd.values != null) {
					StringBuffer vsb =  new StringBuffer();
					vsb.append(" VALUES ?" + key + " { ");
					for (String s : kd.values) {
						vsb.append(s + " ");
					}
					vsb.append(" } ");
					
					sb.append(vsb.toString() + " . ");
				}
			}			
				
			return sb.toString();
		}
		
		public String predefinedValueClauses() {
			StringBuffer sb = new StringBuffer();
			for (Map.Entry<String, KeyData> keye : keys.entrySet()) {
				String key = keye.getKey();
				KeyData kd = keye.getValue();
				if (kd.values != null) {
					sb.append(" VALUES ?" + key + " { ");
					for (String s : kd.values) {
						sb.append(s + " ");
					}
					sb.append(" } ");
				} else {

				}
			}
			for (Map.Entry<String, KeyData> keye : nokeys.entrySet()) {
				String key = keye.getKey();
				KeyData kd = keye.getValue();
				if (kd.values != null) {
					sb.append(" VALUES ?" + key + " { ");
					for (String s : kd.values) {
						sb.append(s + " ");
					}
					sb.append(" } ");
				} else {

				}
			}			
				
			return sb.toString();
		}

		private String gb(String var) {
			if (groupBy) {
				return " (GROUP_CONCAT(DISTINCT " + var + ";separator=\"|\") AS ?g" + var.substring(1) + ") ";
			} else {
				return var;
			}
		}
		
		public String returnVariables(boolean lexicalValue, boolean language) {
			StringBuffer sb = new StringBuffer();
			for (String key : keys.keySet()) {
				sb.append(gb("?" + key + " "));
				if (lexicalValue) {
					sb.append(gb("?lexicalValue_" + key + " "));
				}
				if (language) {
					sb.append(gb("?language_" + key + " "));
				}
			}
				
			return sb.toString();
		}
		
		public String construct(String fromClause, Resource top) {
			StringBuffer sb = new StringBuffer();
			sb.append("CONSTRUCT { ");
			for (String key : keys.keySet()) {
				sb.append("?c_0 <" + prefix + key + "> ?" + key + " . ");
			}
			String stop = "<" + top + "> ";
			sb.append("} " + fromClause + " WHERE { VALUES ?c_0  { " + stop + " } ");
			sb.append(whereClause);
			sb.append(this.predefinedValueClauses());
			sb.append(" } ");
				
			
			return sb.toString();
		}
		
		public String construct(String fromClause, String top) {
			
			StringBuffer sb = new StringBuffer();
			sb.append("CONSTRUCT { ");
			
			Set<String> usedKeys = new HashSet<>();
			
			for (Map.Entry<String, GroupStructure> entry : groups.entrySet()) {
				sb.append("?c_0 <" + prefix  + entry.getKey() + "> ?" + entry.getValue().rootVariable + " . ");
				
				for (String key : entry.getValue().terminalVariables) {
					if (targets != null && targets.get(key) != null ) {
						String prop = targets.get(key);
						if (prop.startsWith("http://") || prop.startsWith("https://")) {
							sb.append("?" + entry.getValue().rootVariable + " <" + prop  + "> ?" + key + " . ");
						} else {
							sb.append("?" + entry.getValue().rootVariable + " <" + prefix + prop  + "> ?" + key + " . ");
						}
					} else {
						sb.append("?" + entry.getValue().rootVariable + " <" + prefix + key + "> ?" + key + " . ");
					}
					
					usedKeys.add(key);

				}
			}
			
			for (String key : keys.keySet()) {
				if (usedKeys.contains(key)) {
					continue;
				}
				
				if (targets != null && targets.get(key) != null ) {
					String prop = targets.get(key);
					if (prop.startsWith("http://") || prop.startsWith("https://")) {
						sb.append("?c_0 <" + prop  + "> ?" + key + " . ");
					} else {
						sb.append("?c_0 <" + prefix + prop  + "> ?" + key + " . ");
					}
				} else {
					sb.append("?c_0 <" + prefix + key + "> ?" + key + " . ");
				}
			}
			String stop = "<" + top + "> ";
			sb.append("} " + fromClause + " WHERE { VALUES ?c_0  { " + stop + " } ");
			sb.append(whereClause);
			sb.append(this.predefinedValueClauses());
			sb.append(" } ");

				
			return sb.toString();
		}
		
		public String construct(String fromClause, List<Resource> top) {
			StringBuffer sb = new StringBuffer();
			sb.append("CONSTRUCT { ");
			for (String key : keys.keySet()) {
				sb.append("?c_0 <" + prefix + key + "> ?" + key + " . ");
			}
			String stop = "";
			for (Resource t : top) {
				stop += "<" + t + "> ";
			}
			sb.append("} " + fromClause + " WHERE { VALUES ?c_0  { " + stop + " } ");
			sb.append(whereClause);
			sb.append(this.predefinedValueClauses());
			sb.append(" } ");
			
//			System.out.println(sb.toString());
				
			return sb.toString();
		}

		public Set<String> getKeys() {
			if (groupBy) {
				Set<String> r = new HashSet<>();
				for (String s : keys.keySet()) {
					r.add("g" + s);
				}
				return r;
			} else {
				return keys.keySet();
			}
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
	
	private class KeyData {
		boolean optional;
		List<String> values;
		
		public KeyData(boolean optional, List<String> values) {
			this.optional = optional;
			if (values != null && values.size() > 0) {
				this.values = values;
			}
		}
		
	}
	private String toSPARQL(ClassIndexElement cie, Map<Integer,IndexKeyMetadata> ikm, String vprefix, int i, List<Resource> path, List<List<Resource>> res, Map<String, KeyData> keys, Map<String, KeyData> nokeys, Map<String, String> targets, Map<String, GroupStructure> groups, GroupStructure currentGroup, boolean optional) {
		StringBuffer sb = new StringBuffer();

		if (cie.getClazz() != null && !cie.getClazz().startsWith("_:")) {
			sb.append("?" + vprefix + "_" + i + " a <" + cie.getClazz().toString() + "> . ");
			path.add(new ResourceImpl(cie.getClazz()));
		}
		
		GroupStructure nextGroup = currentGroup;
		if (cie.getGroup() != null) {
			nextGroup = groups.get(cie.getGroup());
			if (nextGroup == null) {
				nextGroup = new GroupStructure(currentGroup, cie.getGroup(), vprefix + "_" + i);
				groups.put(cie.getGroup(), nextGroup);
			}
		}
		
		if (cie.getProperties() != null) {
			int k = 0;

			for (PropertyIndexElement nie : cie.getProperties()) {
				String prop = nie.getProperty();
				
				if (nie.getSelectors() != null) {
					for (IndexElementSelector ies : nie.getSelectors()) {
						List<String> languages = ies.getLanguages();
			
						String var = "?r" + ies.getIndex();
						String filter = "";
						if (ies.getValues() != null && ies.getValues().size() > 0) {
//							filter += " VALUES " + var + " { ";
//							for (String s : ies.getValues()) {
//								filter += s + " " ;
//							}
//							filter += " } ";
						} else {
							
							if (languages != null) {
								for (int j = 0;  j < languages.size(); j++) {
									if (j > 0) {
										filter +=" || ";
									}
									filter += " langMatches(lang(" + var + "), \"" + languages.get(j) + "\" ) ";
								}
								if (filter.length() > 0) {
									filter = " . FILTER (" + filter + ") ";
								}
								filter += " FILTER (" + (ies.getTermType() == RDFTermType.IRI ? "isIRI" : "isLiteral") + "(" + var + ") ) ";
							}
						}
						
						if (!nie.isRequired()) {
							sb.append(" OPTIONAL { ?" + vprefix + "_" +  i + " " + (nie.isInverse() ? "^" : "") +  "<" + prop.toString() + "> " + var + filter + " } ");
						} else {
							sb.append(" ?" + vprefix + "_" +  i + " " + (nie.isInverse() ? "^" : "") + " <" + prop.toString() + "> " + var + filter + " . ");
						}
						
						if (nextGroup != null) {
							nextGroup.addTerminalVariable(var.substring(1));
						}
						
						if (ikm.containsKey(ies.getIndex())) {
							keys.put("r" + ies.getIndex(), new KeyData(!nie.isRequired(), ies.getValues()));
						} else {
							nokeys.put("r" + ies.getIndex(), new KeyData(!nie.isRequired(), ies.getValues()));
						}
						if (targets != null && ies.getTarget() != null) {
							targets.put("r" + ies.getIndex(), ies.getTarget());
						}
		//				res.add(newPath);
					}
				}

				if (nie.getElements() != null) {
					for (ClassIndexElement ie : nie.getElements()) {
						List<Resource> newPath = new ArrayList<>();
						newPath.addAll(path);
						newPath.add(new PropertyImpl(prop));

//						if (optional) {
						if (!nie.isRequired()) {
							sb.append("OPTIONAL { ?" + vprefix + "_" +  i + " " + (nie.isInverse() ? "^" : "") + " <" + prop.toString() + "> ?" + vprefix + "_" + k + "_0 . " + toSPARQL(ie, ikm, vprefix + "_" + k, 0, newPath, res, keys, nokeys, targets, groups, nextGroup, optional) + "} ");
						} else {
							sb.append("?" + vprefix + "_" +  i + " " + (nie.isInverse() ? "^" : "") +  " <" + prop.toString() + "> ?" + vprefix + "_" + k + "_0 . " + toSPARQL(ie, ikm, vprefix + "_" + k, 0, newPath, res, keys, nokeys, targets, groups, nextGroup, optional) + " ");
						}
						
						k++;
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
				
				String generatorFilter = sparqlQueryService.generatorFilter("v", Arrays.asList(new String[] {edoc.asResource(resourceVocabulary).toString()}));

				for (IndexElementSelector ies : eie.getSelectors()) {
					String var = "?r" + ies.getIndex();
					
					String embedding = 
							" ?v a <" + SEMAVocabulary.EmbeddingAnnotation + "> ; " + 
							"   <" + OAVocabulary.hasTarget + "> ?target ; " + 
							"   <" + OAVocabulary.hasBody + "> " + var + " . " +
							generatorFilter + 
							" ?target <" + OAVocabulary.hasSource + "> ?" + vprefix + "_" + i + " . ";
					
					sb.append("OPTIONAL { " + embedding + " } ");
					
					if (ikm.containsKey(ies.getIndex())) {
						keys.put("r" + ies.getIndex(), new KeyData(false, ies.getValues()));
					} else {
						nokeys.put("r" + ies.getIndex(), new KeyData(false, ies.getValues()));
					}
					
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
	
	private String toTreeSPARQL(ClassIndexElement cie, Map<Integer,IndexKeyMetadata> ikm, List<Resource> path, List<List<Resource>> res, Map<String, KeyData> keys, Map<String, KeyData> nokeys, Map<String, String> targets, Set<String> filters) {
		StringBuffer sb = new StringBuffer();
//		if (clazz.isURIResource()) {
		if (cie.getClazz() != null && !cie.getClazz().startsWith("_:")) {
			sb.append("a <" + cie.getClazz().toString() + "> ; ");
			path.add(new ResourceImpl(cie.getClazz()));
		}
		
		if (cie.getProperties() != null) {
			int k = 0;

			for (PropertyIndexElement nie : cie.getProperties()) {
				String prop = nie.getProperty();
				
				if (nie.getSelectors() != null) {
					for (IndexElementSelector ies : nie.getSelectors()) {
						List<String> languages = ies.getLanguages();
			
						String var = "?r" + ies.getIndex();
						String filter = "";
						if (ies.getValues() != null && ies.getValues().size() > 0) {
							filter += " VALUES " + var + " { ";
							for (String s : ies.getValues()) {
								filter += s + " " ;
							}
							filter += " } ";
						} else {
							
							if (languages != null) {
								for (int j = 0;  j < languages.size(); j++) {
									if (j > 0) {
										filter +=" || ";
									}
									filter += " langMatches(lang(" + var + "), \"" + languages.get(j) + "\" ) ";
								}
								if (filter.length() > 0) {
									filter = ". FILTER (" + filter + ") ";
								}
								filter += "FILTER (" + (ies.getTermType() == RDFTermType.IRI ? "isIRI" : "isLiteral") + "(" + var + ") ) ";
							}
						}
						
						sb.append(" <" + prop.toString() + "> " + var + " ; ");
						
						if (filter.length() > 0) {
							filters.add(filter);
						}
						
						if (ikm.containsKey(ies.getIndex())) {
							keys.put("r" + ies.getIndex(), new KeyData(!nie.isRequired(), ies.getValues()));
						} else {
							nokeys.put("r" + ies.getIndex(), new KeyData(!nie.isRequired(), ies.getValues()));
						}
						if (targets != null && ies.getTarget() != null) {
							targets.put("r" + ies.getIndex(), ies.getTarget());
						}
		//				res.add(newPath);
					}
				}

				if (nie.getElements() != null) {
					int j = 0;
					for (ClassIndexElement ie : nie.getElements()) {
						List<Resource> newPath = new ArrayList<>();
						newPath.addAll(path);
						newPath.add(new PropertyImpl(prop));

						if (j > 0) {
							sb.append("; ");
						}

						sb.append(" <" + prop.toString() + "> [ " + toTreeSPARQL(ie, ikm, newPath, res, keys, nokeys, targets, filters) + " ] ");
						k++;
						j++;
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
