package ac.software.semantic.model;

import java.util.Objects;

import org.apache.jena.JenaRuntime;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Resource;

import com.fasterxml.jackson.annotation.JsonInclude;

import edu.ntua.isci.ac.d2rml.model.Utils;

public class AnnotationEditValue {
	
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String iri;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String lexicalForm;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String language;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String datatype;
	
	public AnnotationEditValue() {
	}

	public AnnotationEditValue(Resource iri) {
		this.iri = iri.toString();
	}

	public AnnotationEditValue(Literal literal) {
		this.lexicalForm = literal.getLexicalForm();
		this.language = literal.getLanguage();
		if (this.language != null && this.language.equals("")) {
			this.language = null;
		}
		this.datatype = literal.getDatatypeURI();
		if (this.datatype != null && this.datatype.equals("")) {
			this.datatype = null;
		}
	}
	
	public String getIri() {
		return iri;
	}

	public void setIri(String iri) {
		this.iri = iri;
	}

	public String getLexicalForm() {
		return lexicalForm;
	}

	public void setLexicalForm(String lexicalForm) {
		this.lexicalForm = lexicalForm;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getDatatype() {
		return datatype;
	}

	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}

	public int hashCode() {
		return Objects.hash(iri, lexicalForm, language, datatype);
	}
	
	public boolean equals(Object obj) {
		
		if (obj instanceof AnnotationEditValue) {
			AnnotationEditValue aev = (AnnotationEditValue)obj;
			return ((iri == null && aev.iri == null) || (iri != null && aev.iri != null && iri.equals(aev.iri))) && 
				   ((lexicalForm == null && aev.lexicalForm == null) || (lexicalForm != null && aev.lexicalForm != null && lexicalForm.equals(aev.lexicalForm))) && 
				   ((language == null && aev.language == null) || (language != null && aev.language != null && language.equals(aev.language)) &&
				   ((datatype == null && aev.datatype == null) || (datatype != null && aev.datatype != null && datatype.equals(aev.datatype))));
		} else {
			return false;
		}
		
	}
	
	public String toString() {
		if (iri != null) {
			return iri;
		} else {
	        StringBuilder b = new StringBuilder() ;
	        b.append('"') ;
		    String lex = Utils.escapeLiteral(lexicalForm) ;
		    b.append(lex) ;
	        b.append('"') ;
		    if (language != null && !language.equals("")) {
		    	b.append("@").append(language) ;
		    } else if (datatype != null && datatype.equals("")) {
		    	if (!(JenaRuntime.isRDF11 && datatype.equals(XSDDatatype.XSDstring.getURI()) ) ) {  
		    		b.append("^^<").append(datatype).append(">") ;
		        }
		    }
		    return b.toString() ;
		}
	}
	
//    protected void writeLiteral(PrintWriter writer) {
//        writer.print('"');
//        writeString(lexicalForm, writer);
//        writer.print('"');
//        if (language != null && !language.equals(""))
//            writer.print("@" + language);
//        if (datatype != null && !datatype.equals(""))
//            writer.print("^^<" + datatype + ">");
//    }
//    
//    private static void writeString(String s, PrintWriter writer) {
//
//        for (int i = 0; i < s.length(); i++) {
//            char c = s.charAt(i);
//            if (c == '\\' || c == '"') {
//                writer.print('\\');
//                writer.print(c);
//            } else if (c == '\n') {
//                writer.print("\\n");
//            } else if (c == '\r') {
//                writer.print("\\r");
//            } else if (c == '\t') {
//                writer.print("\\t");
//            } else if (c >= 32 && c < 127) {
//                writer.print(c);
//            } else {
//                String hexstr = Integer.toHexString(c).toUpperCase();
//                int pad = 4 - hexstr.length();
//                writer.print("\\u");
//                for (; pad > 0; pad--)
//                    writer.print("0");
//                writer.print(hexstr);
//            }
//        }
//    }
}