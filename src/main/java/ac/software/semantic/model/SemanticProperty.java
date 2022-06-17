package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

@Document(collection = "SemanticProperties")
public class SemanticProperty {
   @Id
   private ObjectId id;

   private String uri;
   
   private List<Query> queries;

   private List<Argument> arguments;

   public SemanticProperty(String uri) {
       this.uri = uri;
       
       queries = new ArrayList<>();
       arguments = new ArrayList<>();
   }

   	public ObjectId getId() {
   		return id;
   	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	static Pattern p = Pattern.compile("(@@.+?@@)");
	 
	public void addQuery(String query) {
		List<String> params = new ArrayList<>();
		
		Matcher m = p.matcher(query);
		while (m.find()) {
			params.add(m.group(1));
//			System.out.println(m.group(1));
		}
		
		
		queries.add(new Query(query, params));
	}
	
	public List<Query> getQueries() {
		return queries;
	}

	public void setQueries(List<Query> queries) {
		this.queries = queries;
	}

	public void addArgument(String x) {
		arguments.add(new Argument(x));
	}

	public void addArgument(String x, String... t) {
		arguments.add(new Argument(x, t));
	}

	public List<Argument> getArguments() {
		return arguments;
	}

	public void setArguments(List<Argument> arguments) {
		this.arguments = arguments;
	}
   	

}
