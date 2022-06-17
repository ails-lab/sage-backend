package ac.software.util;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.ntua.isci.ac.common.utils.IOUtils;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.parser.Parser;

import com.fasterxml.jackson.core.JsonGenerator;

public class SerializationTransformation {

	static ObjectMapper mapper = new ObjectMapper();
	
	public Map<String, String> XtoJSONLD(String input) throws Exception {
		
		ObjectNode json = mapper.readValue(input, ObjectNode.class);
		
//		System.out.println("X-JSON");
//		System.out.println(json);
		
		ArrayNode graph = (ArrayNode)json.get("@graph");
		ObjectNode context = (ObjectNode)json.get("@context");
		
		ObjectNode newJson = mapper.createObjectNode();
		ArrayNode newGraph = mapper.createArrayNode();
		ObjectNode newContext = mapper.createObjectNode();
		
		newJson.put("@context", newContext);
		newJson.put("@graph", newGraph);
		
		
		Map<String, String> prefixes = new HashMap<>(); // it is it necessary to add them!
		prefixes.put("rr", "http://www.w3.org/ns/r2rml#");
		prefixes.put("dr", "http://islab.ntua.gr/ns/d2rml#");
		prefixes.put("is", "http://islab.ntua.gr/ns/d2rml-is#");
		prefixes.put("op", "http://islab.ntua.gr/ns/d2rml-op#");
		prefixes.put("http", "http://www.w3.org/2011/http#");
		
		for (Iterator<String> iter = context.fieldNames(); iter.hasNext();) {
			String c = iter.next();
			JsonNode jn = context.get(c);
			if (!jn.isObject()) {
				prefixes.put(c, jn.asText());
			} 
		}

		
		for (Entry<String, String> entry : prefixes.entrySet()) {
//			System.out.println(entry);
			newContext.put(entry.getKey(), entry.getValue());
		}
		
		Map<String, Info> CMap = new HashMap<>();
		
		int counter = 0;
		for (Iterator<JsonNode> iter = graph.elements(); iter.hasNext();) {
			ObjectNode node = (ObjectNode)iter.next();
			
			ObjectNode newNode = mapper.createObjectNode();
			newGraph.add(newNode);
			
			for (Iterator<String> nameIter = node.fieldNames(); nameIter.hasNext();) {
				String name = nameIter.next();
				
				JsonNode node2 = node.get(name);
				
				if (node2.isObject()) {
					if (node2.get("@id") != null) {
						newNode.put(adjustNameForContext(prefixes, true, name, CMap), node2);
					} else if (node2.get("@list") != null) {
						ArrayNode larr = mapper.createArrayNode();
						ArrayNode larrNode = (ArrayNode)node2.get("@list");
						
						ObjectNode lnode = mapper.createObjectNode();
						
						newNode.put(adjustNameForContext(prefixes, true, name, CMap), lnode);
						
						lnode.put("@list", larr);
						
						for (int i = 0 ;i < larrNode.size(); i++) {
							JsonNode node3 = larrNode.get(i);
							if (node3.isObject()) {
								
								if (node3.get("@id") != null) {
									larr.add(node3);
								}  else {
									String bid = "_:b"+ (counter++);
									ObjectNode on = mapper.createObjectNode();
									on.put("@id", bid);
									larr.add(on);

									counter = visit(newGraph, prefixes, CMap, bid, (ObjectNode)node3, counter);
								}
							} else {
								larr.add(node3);
							}
						}
						
						
					} else { 
						String bid = "_:b"+ (counter++);
						newNode.put(adjustNameForContext(prefixes, true, name, CMap), bid);
						counter = visit(newGraph, prefixes, CMap, bid, (ObjectNode)node2, counter);
					}
				} else if (node2.isArray()) {
					ArrayNode arr = mapper.createArrayNode();
					ArrayNode arrNode = (ArrayNode)node2;
					
					boolean id = false;
					
					for (int i = 0 ;i < arrNode.size(); i++) {
						JsonNode node3 = arrNode.get(i);
						if (node3.isObject()) {
							id = true;
							
							if (node3.get("@id") != null) {
								arr.add(node3);
							}  else {
								String bid = "_:b"+ (counter++);
								arr.add(bid);
								counter = visit(newGraph, prefixes, CMap, bid, (ObjectNode)node3, counter);
							}
						} else {
							arr.add(node3);
						}
					}
					newNode.put(adjustNameForContext(prefixes, id, name, CMap), arr);
				} else {
					newNode.put(adjustNameForContext(prefixes, false, name, CMap), node2);
				}
				
			}
		}
		
		for (Entry<String, Info> entry : CMap.entrySet()) {
			ObjectNode tn = mapper.createObjectNode();

			if (entry.getValue().id) {
				tn.put("@id", entry.getValue().full);
				tn.put("@type", "@id");
				newContext.put(entry.getKey(), tn);
			} else if (!prefixes.containsKey(entry.getKey())) {
				newContext.put(entry.getKey(), entry.getValue().full);
				
			}
		}

//		System.out.println("NEW JSON");
//		System.out.println(newJson);
//		System.out.println(cMap);
		
		Map<String, String> res = new HashMap<>();
		res.put(null, newJson.toString());
		
		ArrayNode array = (ArrayNode)json.get("@namedGraphs");
		if (array != null) {
			for (Iterator<JsonNode> iter = array.elements(); iter.hasNext();) {
				JsonNode node = iter.next();
				String ng = node.get("@namedGraph").asText();
				String content = node.get("@content").toString();
				
				res.put(ng, content);
			}
		}
		
		return res;
		
	}
	
	private class Info {
		private String local;
		private String full;
		private boolean id;
		
		public Info(String local, String full, boolean id) {
			this.local = local;
			this.full = full;
			this.id = id;
		}
	}
	
	private String adjustNameForContext(Map<String, String> prefixes, boolean id, String name, Map<String,Info> CMap) throws Exception {
		String newName = name;
		for (Entry<String,String> p : prefixes.entrySet()) {
			
			if (name.startsWith(p.getValue())) {
				
//				System.out.println(">>>> " + name + " " + p);
				
				String local = name.substring(p.getValue().length());
				String pref = p.getKey();
				
				Info info = CMap.get(local);
				if (info != null) {
					if (id == info.id && local.equals(info.local) && name.equals(info.full)) {
					} else {
						
						local = pref + ":" + local;
						
						info = CMap.get(local);
						if (info != null) {
							if (id == info.id && local.equals(info.local) && name.equals(info.full)) {
								newName = local;
							} else {
								if (CMap.containsKey(local)) {
									throw new Exception(local + " already defined.");
								} else {
									info = new Info(local, name, id);
									CMap.put(local, info);
								}
							}
						} else {
							info = new Info(local, name, id);
							CMap.put(local, info);
						}
					}
				} else {
					info = new Info(local, name, id);
					CMap.put(local, info);
				}
				
				newName = local;

				break;
			}
		}
//		System.out.println(">> " + id + " " + name + " > " + newName);
		return newName;
		
	}
	
	private int visit(ArrayNode graph, Map<String, String> prefixes, Map<String, Info> CMap, String id, ObjectNode node, int counter) throws Exception {
		
		ObjectNode newNode = mapper.createObjectNode();
		graph.add(newNode);
		
		newNode.put("@id", id);
		
		for (Iterator<String> nameIter = node.fieldNames(); nameIter.hasNext();) {
			String name = nameIter.next();
			
			JsonNode node2 = node.get(name);
			
			if (node2.isObject()) {
				if (node2.get("@id") != null) {
					newNode.put(adjustNameForContext(prefixes, true, name, CMap), node2);
				} else if (node2.get("@list") != null) {
					ArrayNode larr = mapper.createArrayNode();
					ArrayNode larrNode = (ArrayNode)node2.get("@list");
					
					ObjectNode lnode = mapper.createObjectNode();
					
					newNode.put(adjustNameForContext(prefixes, true, name, CMap), lnode);
					
					lnode.put("@list", larr);
					
					for (int i = 0 ;i < larrNode.size(); i++) {
						JsonNode node3 = larrNode.get(i);
						if (node3.isObject()) {
							
							if (node3.get("@id") != null) {
								larr.add(node3);
							}  else {
								String bid = "_:b"+ (counter++);
								ObjectNode on = mapper.createObjectNode();
								on.put("@id", bid);
								larr.add(on);
								counter = visit(graph, prefixes, CMap, bid, (ObjectNode)node3, counter);
							}
						} else {
							larr.add(node3);
						}
					}				
				}  else {
					String bid = "_:b"+ (counter++);
					newNode.put(adjustNameForContext(prefixes, true, name, CMap), bid);
					counter = visit(graph, prefixes, CMap, bid, (ObjectNode)node2, counter);
				}
			} else if (node2.isArray()) {
				ArrayNode arr = mapper.createArrayNode();
				ArrayNode arrNode = (ArrayNode)node2;
				
				boolean xid = false;
				
				for (int i = 0 ;i < arrNode.size(); i++) {
					JsonNode node3 = arrNode.get(i);
					if (node3.isObject()) {
						xid = true;
						
						if (node3.get("@id") != null) {
							arr.add(node3);
						}  else {
							String bid = "_:b"+ (counter++);
							arr.add(bid);
							counter = visit(graph, prefixes, CMap, bid, (ObjectNode)node3, counter);
						}
					} else {
						arr.add(node3);
					}
				}
				newNode.put(adjustNameForContext(prefixes, xid, name, CMap), arr);
			} else {
				newNode.put(adjustNameForContext(prefixes, false, name, CMap), node2);
			}

		}
		
		return counter;
	}

	public static void main(String[] args) {
//		String path = "D:\\data\\stirdata\\d2rml\\el - business_data - content.ttl";
//		String path = "D:\\data\\stirdata\\d2rml\\el - agencies - data.ttl";
		String path = "D:\\data\\stirdata\\d2rml\\nutsALL - content.ttl";
				
		try
        {
            String content = new String ( Files.readAllBytes( Paths.get(path) ) );
            String x = TTLtoX(content);
            
//            System.out.println(x);
          
        } 
        catch (Exception e) 
        {
            e.printStackTrace();
        }
	}
	
	public static String TTLtoX(String ttl) throws IOException {
		Dataset dataset = DatasetFactory.create();

		RDFDataMgr.read(dataset, new StringReader(ttl), null, Lang.TRIG);
		
//		System.out.println("INPUT");
//		System.out.println(ttl);

		Parser.expand(dataset.getDefaultModel());

//		Writer sw0 = new StringWriter();
//		RDFDataMgr.write(sw0, model, RDFFormat.TTL) ;
//		System.out.println(sw0);
		
		ObjectNode defaultGraph = modelToJson(dataset.getDefaultModel());
		Map<String, ObjectNode> namedGraphs = new HashMap<>();
		
		for (Iterator<String> iter = dataset.listNames(); iter.hasNext();) {
			String graph = iter.next();
//			System.out.println(graph);
			namedGraphs.put(graph, modelToJson(dataset.getNamedModel(graph)));
		}
		
		if (!namedGraphs.isEmpty()) {
			ArrayNode array = mapper.createArrayNode();
			for (Map.Entry<String, ObjectNode> entry : namedGraphs.entrySet()) {
				ObjectNode graphNode = mapper.createObjectNode();
			
				graphNode.put("@namedGraph", entry.getKey());
				graphNode.put("@content", entry.getValue());
				
//				System.out.println(entry.getKey());
//				System.out.println(entry.getValue());
				
				array.add(graphNode);
			}
			
			defaultGraph.put("@namedGraphs", array);
			
		}
		
//		System.out.println(defaultGraph.toString());
		return defaultGraph.toString();
		
	}
	
	
	private static ObjectNode modelToJson(Model model) throws JsonParseException, JsonMappingException, IOException {
		Writer sw = new StringWriter();
		RDFDataMgr.write(sw, model, RDFFormat.JSONLD) ;
//		
//		System.out.println(sw);

		ObjectMapper mapper = new ObjectMapper();
		mapper.getFactory().configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);
		
//		ArrayNode result = mapper.readValue(sw.toString(), ArrayNode.class);
		ObjectNode result = mapper.readValue(sw.toString(), ObjectNode.class);
		
		Map<String, Object[]> idMap = new HashMap<>();
		Map<String, ObjectNode> readyMap = new HashMap<>();
		
		ArrayNode nodes = (ArrayNode)result.get("@graph");
		
		if (nodes != null) {
			
			for (Iterator<JsonNode> iter = nodes.elements(); iter.hasNext(); ) {
				ObjectNode node = (ObjectNode)iter.next();
				
				String nodeId = node.get("@id").asText();
				
				Set<String> set = new HashSet<>();
				explore(node, set);
				
				if (nodeId.startsWith("_:")) {
					((ObjectNode)node).remove("@id");
				} 
				
				if (set.isEmpty()) {
					readyMap.put(nodeId, node);
				} else {
					idMap.put(nodeId, new Object[] {node, set});
				}
			}
		} else {
			
			ObjectNode node = mapper.createObjectNode();
			
			for (Iterator<String> iter = result.fieldNames(); iter.hasNext(); ) {
				String field = iter.next();
				if (field.equals("@context")) {
					continue;
				}
				
				node.put(field, result.get(field));
			}
			
			String nodeId = node.get("@id").asText();
			
			Set<String> set = new HashSet<>();
			explore(node, set);
			
			if (nodeId.startsWith("_:")) {
				((ObjectNode)node).remove("@id");
			} 
			
			if (set.isEmpty()) {
				readyMap.put(nodeId, node);
			} else {
				idMap.put(nodeId, new Object[] {node, set});
			}

		}
		
		
		while (!idMap.isEmpty()) {
			
			for (Iterator<Entry<String, Object[]>> iter = idMap.entrySet().iterator(); iter.hasNext();) {
				Entry<String, Object[]> entry = iter.next();
				
				
				ObjectNode node = (ObjectNode)entry.getValue()[0];
				
				boolean keep = process(node, readyMap);
				
				if (!keep) {
					readyMap.put(entry.getKey(), node);
					iter.remove();
				} 
			}
		}
		
//		System.out.println(sw);
		

		Set<ObjectNode> newMap = new HashSet<>();
		for (Entry<String, ObjectNode> entry : readyMap.entrySet()) {
			if (!entry.getKey().startsWith("_:")) {
				newMap.add(entry.getValue());
			}
		}
		
		
//		Map<String, JsonNode> nameMap = new HashMap<>();
		JsonNode context = (JsonNode)result.get("@context");

		ObjectNode mainJson = mapper.createObjectNode();
		ArrayNode graphJson = mapper.createArrayNode();
		ObjectNode contextJson = mapper.createObjectNode();
		
		for (Iterator<ObjectNode> iter = newMap.iterator(); iter.hasNext();) {
//			ObjectNode nn = update(iter.next(), nameMap);
			ObjectNode nn = iter.next();
			graphJson.add(nn);
		}
		
		contextJson = (ObjectNode)context;
		
		mainJson.put("@graph", graphJson);
		mainJson.put("@context", contextJson);
		
//		System.out.println(mainJson);
		
		return mainJson;

	}
	
	private static void explore(ObjectNode node, Set<String> set) throws JsonParseException, JsonMappingException, IOException {

		for (Iterator<String> iter2 = node.fieldNames(); iter2.hasNext(); ) {
			String name2 = iter2.next();

			JsonNode node2 = node.get(name2);

			if (name2.equals("@id")) {
				continue;
			}
			
			if (node2.isArray()) {
				for (Iterator<JsonNode> iter3 = node2.elements(); iter3.hasNext(); ) {
					JsonNode node3 = iter3.next();
					
					if (node3.isObject()) {
						explore((ObjectNode)node3, set);
					} else {
						String nid = node3.asText();
						
						if (nid.startsWith("_:")) {
							set.add(nid);
						}
					}	
					
				}
			} else if (node2.isObject()) {
				explore((ObjectNode)node2, set);
			} else {
				String nid = node2.asText();
//				System.out.println(nid);				
				
				if (nid.startsWith("_:")) {
//					System.out.println(nid + " " + nid.getClass());
					set.add(nid);
				}
			}
		}
	}
	
	public static boolean process(ObjectNode node, Map<String, ObjectNode> readyMap) {
		
		boolean keep = false;
		
		for (Iterator<String> iter2 = node.fieldNames(); iter2.hasNext(); ) {
			String name2 = iter2.next();
			
			JsonNode node2 = node.get(name2);
			
			if (node2.isArray()) {
				ArrayNode nodea = ((ArrayNode)node2);
				for (int i = 0; i < nodea.size(); i++) {
					JsonNode node3 = nodea.get(i);
					
					if (node3.isObject()) {
						keep |= process((ObjectNode)node3, readyMap);
					} else {
						
						String nid = node3.asText();
						if (nid.startsWith("_:")) {
							JsonNode js = readyMap.get(nid);
							if (js != null) {
								nodea.set(i, js);
							} else {
								keep = true;
							}
						}
					}
					
				}
			} else if (node2.isObject()) {
				keep |= process((ObjectNode)node2, readyMap);
			} else {
				String nid = node2.asText();
				if (nid.startsWith("_:")) {
					JsonNode js = readyMap.get(nid);
					if (js != null) {
						node.replace(name2, js);
					} else {
						keep = true;
					}
				}
			}
		}
		
		return keep;
	}
	
	public static ObjectNode update(ObjectNode node, Map<String, JsonNode> nameMap) {

		ObjectNode result = mapper.createObjectNode();
		
		for (Iterator<String> iter2 = node.fieldNames(); iter2.hasNext(); ) {
			String name2 = iter2.next();
			JsonNode node2 = node.get(name2);

			JsonNode newObj = nameMap.get(name2);
			String newName = null;
			if (newObj != null && newObj.get("@id") != null) {
				newName = newObj.get("@id").asText();
			}
			if (newName == null) {
				newName = name2;
			}

			if (node2.isArray()) {
				ArrayNode nodea = ((ArrayNode)node2);
				ArrayNode newArray = mapper.createArrayNode();
				
				for (int i = 0; i < nodea.size(); i++) {
					JsonNode node3 = nodea.get(i);
					
					if (node3.isObject()) {
						newArray.add(update((ObjectNode)node3, nameMap));
					} else {
						newArray.add(node3);
					}
					
				}
				node2 = newArray;
				
			} else if (node2.isObject()) {
				node2 = update((ObjectNode)node2, nameMap);
			} else if (node2.isTextual()) {
				
				if (newObj != null && newObj.get("@type") != null) {
					if (newObj.get("@type").asText().equals("@id")) {
						String value = nsNormalize(nameMap, node2.asText());
						
						result.put(newName, value);
						continue;
					}
				} else if (name2.equals("@type")) {
					String value = nsNormalize(nameMap, node2.asText());
					result.put(newName, value);
					continue;
				}
			}
			
			result.put(newName, node2);

		}
		
		return result;
	}
	
	private static String nsNormalize(Map<String, JsonNode> ns, String v) {
		for (Entry<String, JsonNode> entry: ns.entrySet()) {
			
			if (entry.getValue().isTextual()) {
				String p = entry.getKey();
				if (v.startsWith(p)) {
					return entry.getValue().asText() + v.substring(p.length() + 1);
				}
			}
		}
		
		return v;
	}
	
	public D2RMLModel XtoD2RMLModel(String xd2rml, Map<String, Object> params) throws Exception {
		Map<String,String> graphs = XtoJSONLD(xd2rml);

		Dataset ds = DatasetFactory.create();

		try (StringReader sr = new StringReader(graphs.get(null))) {
			RDFDataMgr.read(ds, sr, null, Lang.JSONLD);
		}

		for (Map.Entry<String, String> entry : graphs.entrySet()) {
			String graph = entry.getKey();
			if (graph != null) {
				Model model = ModelFactory.createDefaultModel();
				
				try (StringReader sr = new StringReader(entry.getValue())) {
					RDFDataMgr.read(model, sr, null, Lang.JSONLD);
					ds.addNamedModel(graph, model);
				}
		
			}
		}
			
		D2RMLModel res = new Parser().extractRMLMapping(ds.getDefaultModel(), params);
		res.setDataset(ds);
				
		return res;
	}
	

}
