package ac.software.semantic.service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.constants.type.MappingType;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.D2RMLValidateResponse;
import ac.software.semantic.vocs.SEMRVocabulary;
import edu.ntua.isci.ac.d2rml.model.D2RMLFactory;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.parser.Parser;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLISVocabulary;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLOPVocabulary;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.R2RMLVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;


@Service
public class D2RMLService {

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	public D2RMLModel prepare(MappingDocument mapping, ac.software.semantic.model.Dataset dataset, String baseUri, Map<String, Object> params) throws Exception {

		Dataset ds = DatasetFactory.create();

		String d2rml = mapping.getFileContents().replaceAll("\\{@@SAGE_TEMPLATE_DATASET_URI@@\\}", "{@@NO_URL_ENCODE@@SAGE_TEMPLATE_DATASET_URI@@}");
		
		try (StringReader sr = new StringReader(d2rml)) {
			RDFDataMgr.read(ds, sr, baseUri, Lang.TRIG);
		}
		
		Model model = ds.getDefaultModel();
		
		UpdateRequest ur = UpdateFactory.create();

		// delete graph maps
		ur.add("DELETE { ?subject <" + R2RMLVocabulary.graph + "> ?object } WHERE { ?subject <" + R2RMLVocabulary.graph + "> ?object }");
		ur.add("DELETE { ?subject <" + R2RMLVocabulary.graphMap + "> ?object } WHERE { ?subject <" + R2RMLVocabulary.graphMap + "> ?object }"); // possibly dangling nodes. may be a problem

		if (mapping.getType() == MappingType.HEADER) {
			Parser.expand(model);
			
//			RDFDataMgr.write(System.out, model, Lang.TRIG);
			
			// add subject to metadata mapping // legacy // not correct when specification exists
			ur.add("DELETE { ?subject ?prop  ?propValue } " + 
				   "INSERT { ?subject <" + R2RMLVocabulary.constant + "> <" + resourceVocabulary.getDatasetContentAsResource(dataset) + ">  } " +
			       "WHERE { ?dataset <" + D2RMLVocabulary.triplesMap + ">/<" + R2RMLVocabulary.subjectMap + "> ?subject . ?subject ?prop ?propValue . " + 
			       "VALUES ?prop { <" + R2RMLVocabulary.constant + "> <" + R2RMLVocabulary.column + "> <" + R2RMLVocabulary.template + "> } . FILTER NOT EXISTS { ?root ?property ?dataset }  }");
			
		}
		
		UpdateAction.execute(ur, model);

		D2RMLModel res = new Parser().extractRMLMapping(ds.getDefaultModel(), params);
		
		res.setParseParams(params);
		
		res.setDataset(ds);
				
		return res;
	}	
	
	public D2RMLValidateResponse validate(String d2rml) throws Exception {
    	Dataset dataset = DatasetFactory.create();
    	
    	try (InputStream is = new ByteArrayInputStream(d2rml.getBytes())) {
    		RDFDataMgr.read(dataset, is, Lang.TRIG);

    		Model model = dataset.getDefaultModel();

        	Resource subj = null;

        	StmtIterator statements = model.listStatements((Resource)null, RDFVocabulary.type, D2RMLVocabulary.D2RMLSpecification);
	        while (statements.hasNext()) {
	        	Statement stmt = statements.next();
	        	subj = stmt.getSubject();
	        	break;
	        }
	        
//	        if (subj == null) {
//	        	statements = model.listStatements((Resource)null, RDFVocabulary.type, D2RMLVocabulary.D2RMLDocument);
//		        while (statements.hasNext()) {
//		        	Statement stmt = statements.next();
//		        	subj = stmt.getSubject();
//		        	break;
//		        }
//	        }
	        
	        if (subj == null) {
	        	statements = model.listStatements((Resource)null, D2RMLVocabulary.logicalDatasets, (RDFNode)null);
		        while (statements.hasNext()) {
		        	Statement stmt = statements.next();
		        	subj = stmt.getSubject();
		        	break;
		        }
	        }
	         
	        List<DataServiceParameter> parameters = new ArrayList<>();
	        List<String> dependencies = new ArrayList<>();
	        if (subj != null) {
	        	
	        	for (RDFNode obj : D2RMLFactory.getAlternativeListElements(model, subj, D2RMLVocabulary.parameter, D2RMLVocabulary.parameters)) {
	        	
	        		DataServiceParameter pm = null;
	        		
		        	StmtIterator nameStatements = model.listStatements(obj.asResource(), D2RMLOPVocabulary.name, (RDFNode)null);
			        if (nameStatements.hasNext()) {
			        	Statement pstmt = nameStatements.next();
			        	pm = new DataServiceParameter(pstmt.getObject().asLiteral().getLexicalForm());
//			        	name = pstmt.getObject().asLiteral().getLexicalForm();
//		        		parameters.add(name + (optional ? "*" : ""));
			        }

			        if (pm == null) {
			        	continue;
			        }
			        
			        parameters.add(pm);

		        	StmtIterator optionalStatements = model.listStatements(obj.asResource(), D2RMLOPVocabulary.optional, (RDFNode)null);
			        if (optionalStatements.hasNext()) {
			        	pm.setRequired(!optionalStatements.next().getObject().asLiteral().getBoolean());
			        } else {
			        	optionalStatements = model.listStatements(obj.asResource(), D2RMLVocabulary.optional, (RDFNode)null);
				        if (optionalStatements.hasNext()) {
				        	pm.setRequired(!optionalStatements.next().getObject().asLiteral().getBoolean());
				        } else {			        	
				        	pm.setRequired(true);
				        }
			        }
			        
		        	StmtIterator defaultValueStatements = model.listStatements(obj.asResource(), D2RMLOPVocabulary.defaultValue, (RDFNode)null);
			        if (defaultValueStatements.hasNext()) {
			        	pm.setDefaultValue(defaultValueStatements.next().getObject().toString());
			        } else {
			        	defaultValueStatements = model.listStatements(obj.asResource(), D2RMLVocabulary.defaultValue, (RDFNode)null);
				        if (defaultValueStatements.hasNext()) {
				        	pm.setDefaultValue(defaultValueStatements.next().getObject().toString());
				        }
			        }
			        
			        StmtIterator datatypeStatements = model.listStatements(obj.asResource(), D2RMLOPVocabulary.datatype, (RDFNode)null);
			        if (datatypeStatements.hasNext()) {
			        	Statement dstmt = datatypeStatements.next();
			        	pm.setDatatype(dstmt.getObject().asResource().toString());
			        	if (dstmt.getObject().asResource().equals(D2RMLISVocabulary.RDFDataset)) {
			        		dependencies.add(pm.getName());
			        	}
			        }
			        
			        StmtIterator formatStatements = model.listStatements(obj.asResource(), D2RMLISVocabulary.fileFormat, (RDFNode)null);
			        List<String> formats = new ArrayList<>();
			        while (formatStatements.hasNext()) {
			        	Statement dstmt = formatStatements.next();
			        	formats.add(dstmt.getObject().asResource().toString());
			        }
			        if (formats.size() > 0) {
			        	pm.setFormat(formats);
			        }
			        
			        StmtIterator descriptionStatements = model.listStatements(obj.asResource(), D2RMLOPVocabulary.description, (RDFNode)null);
			        if (descriptionStatements.hasNext()) {
			        	Statement dstmt = descriptionStatements.next();
			        	pm.setDescription(dstmt.getObject().asLiteral().getLexicalForm());
			        }
	        	}
	        	
//	        	statements = model.listStatements(subj, D2RMLVocabulary.parameter, (RDFNode)null);
//	        	
//		        while (statements.hasNext()) {
//		        	Statement stmt = statements.next();
//		        	RDFNode obj = stmt.getObject();
//		        	
//		        	String name = null;
//		        	StmtIterator pstatements = model.listStatements(obj.asResource(), D2RMLOPVocabulary.name, (RDFNode)null);
//			        if (pstatements.hasNext()) {
//			        	Statement pstmt = pstatements.next();
//			        	name = pstmt.getObject().asLiteral().getLexicalForm();
//			        	parameters.add(name);
//			        }
//			        
//			        StmtIterator dstatements = model.listStatements(obj.asResource(), D2RMLOPVocabulary.datatype, (RDFNode)null);
//			        if (dstatements.hasNext()) {
//			        	Statement dstmt = dstatements.next();
//			        	if (dstmt.getObject().asResource().equals(D2RMLISVocabulary.RDFDataset)) {
//			        		dependencies.add(name);
//			        	}
//			        }
//		        }
//		        
//		        statements = model.listStatements(subj, D2RMLVocabulary.parameters, (RDFNode)null);
//
//		        while (statements.hasNext()) {
//		        	Statement stmt = statements.next();
//		        	RDFNode obj = stmt.getObject();
//		        	
//		        	for (RDFNode node : D2RMLFactory.extractList(model, obj.asResource())) {
//		        		
//		        		String name = null;
//			        	StmtIterator pstatements = model.listStatements(node.asResource(), D2RMLOPVocabulary.name, (RDFNode)null);
//				        if (pstatements.hasNext()) {
//				        	Statement pstmt = pstatements.next();
//				        	name = pstmt.getObject().asLiteral().getLexicalForm();
//				        	parameters.add(name);
//				        }
//				        
//				        StmtIterator dstatements = model.listStatements(node.asResource(), D2RMLOPVocabulary.datatype, (RDFNode)null);
//				        if (dstatements.hasNext()) {
//				        	Statement dstmt = dstatements.next();
//				        	if (dstmt.getObject().asResource().equals(D2RMLISVocabulary.RDFDataset)) {
//				        		dependencies.add(name);
//				        	}
//				        }
//		        	}
//		        }
	        }
	        
	        D2RMLValidateResponse res = new D2RMLValidateResponse();
	        if (!parameters.isEmpty()) {
	        	res.setParameters(parameters);
	        }
	        if (!dependencies.isEmpty()) {
	        	res.setDependencies(dependencies);
	        }
	        
	        return res;
    	}
	}
}
