package ac.software.semantic.service;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.URI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleLiteral;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import edu.ntua.isci.ac.common.db.QueryResult;

public class RDF4JAccessWrapper implements RDFAccessWrapper, AutoCloseable {

	private Repository ds;
	
	private TupleQueryResult rs;
	
	private List<String> args;
	private int size;
	
	private RepositoryConnection conn;
	private BindingSet sol;
	
	RDF4JAccessWrapper() {
		ds = new SailRepository(new MemoryStore());
	}
	
	public void load(File f) throws Exception {
		try (RepositoryConnection conn = ds.getConnection()) {
			conn.add(f, null, RDFFormat.TRIG);
		}
	}
	
	@Override
	public void execQuery(String query) {
		conn = ds.getConnection();
		
		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
		TupleQueryResult rs = tupleQuery.evaluate();
		this.rs = rs;

		args = rs.getBindingNames();
		size = args.size();
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
//		
//		QueryResult r = new QueryResult(size);
//		for (int i = 0; i < size; i++) {
//			
//			Value v = sol.getBinding(args.get(i)).getValue();
//
//			String sv = v.toString();
//			
//			System.out.println(v.getClass());
//			if (v instanceof URI) {
//				sv = "<" + sv + ">";
//			} else {
//				
//			}
//			
//			r.set(i, sv);
//		}
//		
//		return r;
	}

	@Override
	public void close() {
		if (rs != null) {
			rs.close();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	@Override
	public RDFNode get(String name) {
		Binding bind = sol.getBinding(name);

		if (bind == null) {
			return null;
		}
		
		Value v = bind.getValue();
		
		if (v instanceof SimpleLiteral) {
			SimpleLiteral literal = (SimpleLiteral)v;
			
			String label = literal.getLabel();
			Optional<String> language = literal.getLanguage();
			IRI datatype = literal.getDatatype();
			
			if (datatype == null) {
				if (!language.isPresent()) {
					return ResourceFactory.createPlainLiteral(label);
				} else {
					return ResourceFactory.createLangLiteral(label, language.get());
				}
			} else {
				if (!language.isPresent()) {
					return ResourceFactory.createTypedLiteral(label);
				} else {
					return ResourceFactory.createTypedLiteral(label, null);
				}
			}
		} else if (v instanceof IRI) {
			return ResourceFactory.createResource(v.toString());
		}
		
		System.out.println(v.getClass());
		return null;
	}	
}
