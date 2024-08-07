package ac.software.semantic.payload.response;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatabaseResponse {
   private String id;

   private String name;
   
   private String label;
   
   private String resourcePrefix;
   
   private String lodview;
   
   private List<String> tripleStores;
   private List<String> indexEngines;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getResourcePrefix() {
		return resourcePrefix;
	}

	public void setResourcePrefix(String resourcePrefix) {
		this.resourcePrefix = resourcePrefix;
	}

	public String getLodview() {
		return lodview;
	}

	public void setLodview(String lodview) {
		this.lodview = lodview;
	}

	public List<String> getTripleStores() {
		return tripleStores;
	}

	public void setTripleStores(Collection<String> tripleStores) {
		this.tripleStores = new ArrayList<>();
		this.tripleStores.addAll(tripleStores);
	}

	public List<String> getIndexEngines() {
		return indexEngines;
	}

	public void setIndexEngines(Collection<String> indexEngines) {
		this.indexEngines = new ArrayList<>();
		this.indexEngines.addAll(indexEngines);

	}


}
