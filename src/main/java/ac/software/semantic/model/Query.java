package ac.software.semantic.model;

import java.util.List;

public class Query {
	   private String query;
	   private List<String> parameters;
	
	   public Query(String query, List<String> parameters) {
		   this.query = query;
		   this.parameters = parameters;
	   }
	   
	   public String getQuery() {
		   return query;
	   }
	
	   public void setQuery(String query) {
		   this.query = query;
	   }
	
	   public List<String> getParameters() {
		   return parameters;
	   }
	   
	   public void setParameters(List<String> parameters) {
		   this.parameters = parameters;
	   }
	   
}