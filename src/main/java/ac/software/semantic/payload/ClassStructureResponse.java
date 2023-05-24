package ac.software.semantic.payload;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.rdf.model.Resource;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import ac.software.semantic.service.SchemaService.ClassStructure;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ClassStructureResponse {
	
	@JsonProperty("class")
	private String clazz;
	
	private String property;
	
	private List<String> range;
	
	private List<ClassStructureResponse> children;
	
	private List<EmbedderInfo> embedders;
	
	private int depth;
	
	class EmbedderInfo {
		private String embedderId;
		private String embedder;
		
		EmbedderInfo() {}
		
		EmbedderInfo(String embedderId, String embedder) {
			this.embedderId = embedderId;
			this.embedder = embedder;
		}

		public String getEmbedderId() {
			return embedderId;
		}

		public void setEmbedderId(String embedderId) {
			this.embedderId = embedderId;
		}

		public String getEmbedder() {
			return embedder;
		}

		public void setEmbedder(String embedder) {
			this.embedder = embedder;
		}
	}
	
	ClassStructureResponse() {
	}

	public String getClazz() {
		return clazz;
	}

	public void setClazz(String clazz) {
		this.clazz = clazz;
	}

	public int getDepth() {
		return depth;
	}

	public void setDepth(int depth) {
		this.depth = depth;
	}

	public String getProperty() {
		return property;
	}

	public void setProperty(String property) {
		this.property = property;
	}

	public List<ClassStructureResponse> getChildren() {
		return children;
	}
	
	public void setChildren(List<ClassStructureResponse> children) {
		this.children = children;
	}

	public static ClassStructureResponse createFrom(ClassStructure cs) {
		ClassStructureResponse csr = new ClassStructureResponse();
		if (cs.getClazz() != null) {
			csr.setClazz(cs.getClazz().toString());
		}
		if (cs.getProperty() != null) {
			csr.setProperty(cs.getProperty().toString());
		}
		csr.setDepth(cs.getDepth());
		
		if (cs.getRange() != null) {
			csr.range = new ArrayList<>();
			for (Resource r : cs.getRange()) {
				csr.range.add(r.toString());
			}
		}
		
		if (cs.getChildren() != null) {
			List<ClassStructureResponse> children = new ArrayList<>();
			for (ClassStructure c : cs.getChildren().values()) {
				ClassStructureResponse cc = ClassStructureResponse.createFrom(c);
				children.add(cc);
			}
			csr.setChildren(children);
		}
		
		return csr;
	}

	public List<String> getRange() {
		return range;
	}

	public void setRange(List<String> range) {
		this.range = range;
	}

	public List<EmbedderInfo> getEmbedders() {
		return embedders;
	}

	public void addEmbedder(String embedderId, String embedder) {
		if (embedders == null) {
			embedders = new ArrayList<>();
		}
		
		embedders.add(new EmbedderInfo(embedderId, embedder));
		
	}
}