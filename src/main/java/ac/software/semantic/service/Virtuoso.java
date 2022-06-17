package ac.software.semantic.service;

import java.util.Map;

import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import ac.software.semantic.model.VirtuosoConfiguration;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;
import virtuoso.jena.driver.VirtuosoUpdateFactory;

@Component
public class Virtuoso {

	Logger logger = LoggerFactory.getLogger(Virtuoso.class);
	
//    @Autowired
//    @Qualifier("virtuoso-configuration")
//	private Map<String,VirtuosoConfiguration> virtuosoConfiguration;
  
//    @Value("${virtuoso.isql.username}")
//    private String username;
//
//    @Value("${virtuoso.isql.password}")
//    private String password;
    
    public Virtuoso() { }
    
    public VirtGraph getVirtGraph(VirtuosoConfiguration vc) {
    	return new VirtGraph(vc.getIsqlLocation(), vc.getIsqlUsername(), vc.getIsqlPassword());
    }

    public VirtGraph getVirtGraph(VirtuosoConfiguration vc, String graph) {
    	return new VirtGraph(graph, vc.getIsqlLocation(), vc.getIsqlUsername(), vc.getIsqlPassword());
    }
    
    public void nestedDelete(VirtuosoConfiguration vc, String graph, String uri) {
    	
    	logger.info("NESTED DELETE " + graph + " " + uri);
    	
    	VirtGraph vgraph = getVirtGraph(vc);
    	
		StringBuffer sb = null;
		int i;
		for (i = 1; ; i++) {
			sb = new StringBuffer();
			
			sb.append("SELECT DISTINCT COUNT(?o" + i + ")  FROM <" + graph + "> WHERE { <" + uri + "> ?p1 ?o1 . ");
			for (int j = 2; j <= i; j++) {
				sb.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
			}
			for (int j = 2; j <= i; j++) {
				sb.append(" } ");
			}
			
			sb.append("}");
			
			try (VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(QueryFactory.create(sb.toString(), Syntax.syntaxARQ), vgraph)) {
				ResultSet results = vqe.execSelect();
				String name = results.getResultVars().get(0);
				
//				System.out.println(sb);
	
				QuerySolution qs = results.next();
	
//				System.out.println(qs.get(name));
	
				if (qs.get(name).asLiteral().getInt() == 0) {
					break;
				}
			}
		}
		
		StringBuffer db = null;
		for (int k = 1; k < i; k++) {
			db = new StringBuffer();
			
			db.append("WITH <" + graph + "> DELETE { ?s ?p1 ?o1 . ");
			for (int j = 2; j <= k; j++) {
				db.append(" ?o" + (j-1) + " ?p" + j + " ?o" + j + ". ");
			}
			db.append("} WHERE { VALUES ?s { <" + uri + "> }  ?s ?p1 ?o1 . ");
			for (int j = 2; j <= k; j++) {
				db.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
			}			
			for (int j = 2; j <= k; j++) {
				db.append(" } ");
			}
			db.append("}");
			
		}
		
		if (db != null) {
			System.out.println(db.toString());
			
			VirtuosoUpdateFactory.create(db.toString(), vgraph).exec();
		}
		
		vgraph.close();

    }
    
    
    

}
