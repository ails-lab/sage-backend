package ac.software.semantic.service;

import java.io.File;
import java.util.List;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import edu.ntua.isci.ac.common.db.QueryResult;

public class JenaAccessWrapper implements RDFAccessWrapper, AutoCloseable {
	
	private Dataset ds;
	
	private QueryExecution qe;
	private ResultSet rs; 
	
	private List<String> args;
	
	private QuerySolution sol;
	
	public JenaAccessWrapper() {
		ds = DatasetFactory.create();
	}
	
	
	public Dataset getDataset() {
		return ds;
	}
	
	@Override
	public void load(File f) throws Exception {
		RDFDataMgr.read(ds, "file:" + f.getCanonicalPath(), null, Lang.TRIG);
	}
	
	@Override
	public void execQuery(String query) {
		qe = QueryExecutionFactory.create(QueryFactory.create(query, Syntax.syntaxSPARQL_11), ds);
		rs = qe.execSelect();
		
		args = rs.getResultVars();
	}
	
	@Override
	public boolean hasNext() {
		if (rs == null) {
			return false;
		}
	
		return rs.hasNext();
	}
	
	@Override
	public void next() {
		sol = rs.next();
		
//		QueryResult r = new QueryResult(args.size());
//		for (int i = 0; i < args.size(); i++) {
//
//			RDFNode s = sol.get(args.get(i));
//			r.set(i, s);
//		}
//
//		return r;
	}
	
	@Override
	public RDFNode get(String name) {
		return sol.get(name);
	}

	@Override
	public void close() {
		if (qe != null) {
			qe.close();
			qe = null;
		}
	}
}
