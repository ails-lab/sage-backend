package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.type.PathElementType;
import ac.software.semantic.payload.PrefixizedUri;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PathElement extends PrefixizedUri {
	private PathElementType type;
	
	public PathElement(PathElementType type, String uri) {
		super(uri);
		this.type = type;
	}
	
	public static PathElement createPropertyPathElement(String uri) {
		return new PathElement(PathElementType.PROPERTY, uri);
	}

	public static PathElement createClassPathElement(String uri) {
		return new PathElement(PathElementType.CLASS, uri);
	}
	
	public PathElementType getType() {
		return type;
	}

	public void setType(PathElementType type) {
		this.type = type;
	}

	
	@JsonIgnore
	public boolean isProperty() {
		return type == PathElementType.PROPERTY;
	}

	@JsonIgnore
	public boolean isClass() {
		return type == PathElementType.CLASS;
	}


//	public static String[] onPropertyList(List<PathElement> path) {
//		// TODO: check if in right order.
//		List<String> spath = new ArrayList<>();
//		for (int i = 0; i < path.size(); i++) {
//			if (path.get(i).type == PathElementType.CLASS) {
//				continue;
//			}
//
//			spath.add(path.get(i).uri);
//		}
//
//		return spath.toArray(new String[] {});
//	}

//	public static String onPropertyListAsString(List<PathElement> path) {
//		// TODO: check if in right order.
//		String spath = "";
//		for (int i = 0; i < path.size(); i++) {
//			if (path.get(i).isClass()) {
//				continue;
//			}
//
//			if (spath.length() > 0) {
//				spath += "/";
//			}
//			spath += "<" + path.get(i).uri + ">";
//		}
//
//		return spath;
//	}
	
	// preeceed class uris by RDFSClass.
	public static List<String> onPathElementListAsStringList(List<PathElement> path) {
		List<String> spath = new ArrayList<>();
		for (int i = 0; i < path.size(); i++) {
			if (path.get(i).isClass()) {
				spath.add(RDFSVocabulary.Class.toString());
			}
			
			spath.add(path.get(i).getUri());
		}
	
		return spath;
	}
	
	public static List<PathElement> onPathElementListAsStringListInverse(List<String> path, VocabularyContainer vocc) {
		List<PathElement> spath = new ArrayList<>();
		
		for (int i = 0; i < path.size(); ) {
			PathElement pe;
			if (path.get(i).equals(RDFSVocabulary.Class.toString())) {
				pe = createClassPathElement(path.get(++i));
			} else {
				pe = createPropertyPathElement(path.get(i));
			}

			spath.add(pe);
			if (vocc != null) {
				PrefixizedUri pu = vocc.arrayPrefixize(pe.getUri());
				if (pu.getNamespace() != null) {
					pe.setNamespace(pu.getNamespace());
					pe.setPrefix(pu.getPrefix());
					pe.setLocalName(pu.getLocalName());
				}
			}
			
			i++;
		}
	
		return spath;
	}
	
	public static String onPathStringListAsSPARQLString(List<String> path) {
		return onPathStringListAsSPARQLString(path, "?VAR_ZZZ_");
	}
	
	public static String onPathStringListAsSPARQLString(List<String> path, String var) {
		String s = "";
		
//		int count = 0;
		int count = path.size();
		boolean classBefore = false;
		for (int i = 0; i < path.size(); ) {
			if (i > 0 && !classBefore) {
				s += var + count + " ";
			}
			if (path.get(i).equals(RDFSVocabulary.Class.toString())) {
				s += "a <" + path.get(++i) + "> ; ";
				classBefore = true;
			} else {
//				s += "<" + path.get(i) + "> " + (i < path.size() - 1 ? var + ++count + " . ": " ");
				s += "<" + path.get(i) + "> " + (i < path.size() - 1 ? var + --count + " . ": " ");
				classBefore = false;
			}
			i++;
		}
	
		return s.trim();
	}
	
//	public static String onPathStringListAsPrettyString(List<String> path) {
//		String s = "";
//		
//		boolean beforeClass = false;
//		for (int i = 0; i < path.size(); ) {
//			if (!beforeClass && i > 0) {
//				s += "/" ;
//			}
//			if (path.get(i).equals(RDFSVocabulary.Class.toString())) {
//				s += "[" + path.get(++i) + "]";
//				beforeClass = true;
//			} else {
//				s += "<" + path.get(i) + ">";
//				beforeClass = false;
//			}
//			i++;
//		}
//	
//		return s.trim();
//	}
	
	
	
	public static String onPathStringListAsMiddleRDFPath(List<String> path) {
		String s = "";
		
		for (int i = 0; i < path.size();) {
			if (s.length() > 0 ) {
				s += "/" ;
			}
			if (path.get(i).equals(RDFSVocabulary.Class.toString())) {
				if (i == 0) {
					i++;
				}
			} else {
				s += "<" + path.get(i) + ">";
			}
			i++;
		}
	
		return "/" + s.trim() + "/";
	}
	
	public static int onPathLength(List<String> path) {
		int c = 0;
		
		for (int i = 0; i < path.size();) {
			if (path.get(i).equals(RDFSVocabulary.Class.toString())) {
				if (i == 0) {
					i++;
				}
			} else {
				c++;
			}
			i++; 
		}
	
		return c;
	}
	
	public static String onPathElementListAsString(List<PathElement> path) {
		// TODO: legacy was inversed [apollonis]
		String spath = "";
		
		boolean clazz = false;
		for (int i = 0 ; i < path.size(); i++) {
			if (!clazz && i < path.size() - 1) {
				spath += "/";
			}
			spath += "<" + path.get(i).getUri() + ">";
			
			if (path.get(i).isClass()) {
				clazz = true;
			} else {
				clazz = false;
			}
		}

		return spath;
	}


}