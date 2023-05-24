package ac.software.semantic.controller;


import io.swagger.v3.oas.annotations.Parameter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.D2RMLValidateResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import edu.ntua.isci.ac.d2rml.model.D2RMLFactory;
import edu.ntua.isci.ac.d2rml.model.informationsource.CurrentSource;
import edu.ntua.isci.ac.d2rml.model.map.Transformation;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLISVocabulary;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLOPVocabulary;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "D2RML API")

@RestController
@RequestMapping("/api/d2rml")
public class APID2RMLController {

//	public static void main(String[] args) throws Exception {
//		
////		String s = IOUtils.readFileAsString(new File("D:\\data\\apollonis\\data\\aski\\import.ttl"), "UTF-8");
//		String s = IOUtils.readFileAsString(new File("D:\\data\\apollonis\\data\\aski\\test.ttl"), "UTF-8");
//	
//	Model model = ModelFactory.createDefaultModel();
//	
//			RDFDataMgr.read(model, new StringReader(s), null, Lang.TURTLE);
//			
//			Writer sw = new StringWriter();
//			
////			RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
//			RDFDataMgr.write(sw, model, RDFFormat.JSONLD) ;
//
//			System.out.println("GOOD JSON");
//
//			String goodJson = sw.toString();
//			System.out.println(sw);
//		
//		String d2rml = 
//				
//			"{\"@graph\":[{\"@id\":\"file:///D:/data/software/semantic/#Base\",\"@type\":\"http://islab.ntua.gr/ns/d2rml-is#FileSource\",\"http://islab.ntua.gr/ns/d2rml-is#encoding\":\"UTF8\",\"http://islab.ntua.gr/ns/d2rml-is#path\":[\"D://data//apollonis//data//aski//data//aski_1940-1949.xml\"]},{\"@id\":\"file:///D:/data/software/semantic/#Header\",\"http://www.w3.org/ns/r2rml#graphMap\":[{\"http://www.w3.org/ns/r2rml#constant\":{\"@id\":\"http://sw.islab.ntua.gr/apollonis/core/graph\"}}],\"http://www.w3.org/ns/r2rml#subjectMap\":{\"http://www.w3.org/ns/r2rml#class\":[{\"@id\":\"kvoc:DataCollection\"}],\"http://www.w3.org/ns/r2rml#constant\":\"alex\"}}],\"@context\":{\"class\":{\"@id\":\"http://www.w3.org/ns/r2rml#class\",\"@type\":\"@id\"},\"constant\":{\"@id\":\"http://www.w3.org/ns/r2rml#constant\",\"@type\":\"@id\"},\"subjectMap\":{\"@id\":\"http://www.w3.org/ns/r2rml#subjectMap\",\"@type\":\"@id\"},\"graph\":{\"@id\":\"http://www.w3.org/ns/r2rml#graph\",\"@type\":\"@id\"},\"encoding\":{\"@id\":\"http://islab.ntua.gr/ns/d2rml-is#encoding\"},\"path\":{\"@id\":\"http://islab.ntua.gr/ns/d2rml-is#path\"},\"rr\":\"http://www.w3.org/ns/r2rml#\",\"is\":\"http://islab.ntua.gr/ns/d2rml-is#\",\"collection\":\"http://sw.islab.ntua.gr/apollonis/dc/aski/digital-archive/\",\"kvoc\":\"http://sw.islab.ntua.gr/apollonis/ms/\"}}";
//
//		
//		String nj = XtoJSONLD(d2rml);
//
//		RDFSource ts = new RDFJenaModelSource();
//
//		Model model2 = ModelFactory.createDefaultModel();
////
//		RDFDataMgr.read(model2, new StringReader(nj), null, Lang.JSONLD);
////		RDFDataMgr.read(model2, new StringReader(goodJson), null, Lang.JSONLD);
////		
//        StringWriter sw2 = new StringWriter();
//        RDFDataMgr.write(sw2, model2, RDFFormat.TRIG);
//        
//        
//        System.out.println("NEW TURTLE");
//        System.out.println(sw2.toString());
//        
//	}
	
//
//	
//    @PostMapping(value = "/ttl2jsonldx", produces = "application/json")
//    public String ttl2jsonldx(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestBody String ttl) throws Exception {
//		return SerializationTransformation.TTLtoX(ttl);
//    }
	

    @PostMapping(value = "/validate", produces = "application/json")
    public ResponseEntity<?> validate(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestBody String d2rml) throws Exception {
    	
    	try {
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
		        
		        if (subj == null) {
		        	statements = model.listStatements((Resource)null, RDFVocabulary.type, D2RMLVocabulary.D2RMLDocument);
			        while (statements.hasNext()) {
			        	Statement stmt = statements.next();
			        	subj = stmt.getSubject();
			        	break;
			        }
		        }
		        
		        if (subj == null) {
		        	statements = model.listStatements((Resource)null, D2RMLVocabulary.logicalDatasets, (RDFNode)null);
			        while (statements.hasNext()) {
			        	Statement stmt = statements.next();
			        	subj = stmt.getSubject();
			        	break;
			        }
		        }
		         
		        List<String> parameters = new ArrayList<>();
		        if (subj != null) {
		        	statements = model.listStatements(subj, D2RMLVocabulary.parameter, (RDFNode)null);
		        	
			        while (statements.hasNext()) {
			        	Statement stmt = statements.next();
			        	RDFNode obj = stmt.getObject();
			        	
			        	StmtIterator pstatements = model.listStatements(obj.asResource(), D2RMLOPVocabulary.name, (RDFNode)null);
				        while (pstatements.hasNext()) {
				        	Statement pstmt = pstatements.next();
				        	parameters.add(pstmt.getObject().asLiteral().getLexicalForm());
				        }
			        }
			        
			        statements = model.listStatements(subj, D2RMLVocabulary.parameters, (RDFNode)null);

			        while (statements.hasNext()) {
			        	Statement stmt = statements.next();
			        	RDFNode obj = stmt.getObject();
			        	
			        	for (RDFNode node : D2RMLFactory.extractList(model, obj.asResource())) {
				        	StmtIterator pstatements = model.listStatements(node.asResource(), D2RMLOPVocabulary.name, (RDFNode)null);
					        while (pstatements.hasNext()) {
					        	Statement pstmt = pstatements.next();
					        	parameters.add(pstmt.getObject().asLiteral().getLexicalForm());
					        }
			        	}
			        }
		        }
		        
		        D2RMLValidateResponse res = new D2RMLValidateResponse();
		        if (!parameters.isEmpty()) {
		        	res.setParameters(parameters);
		        }
		        
		        return ResponseEntity.ok(res);
		        
	    	} catch (Exception ex) {
	    		ex.printStackTrace();
	    		return new ResponseEntity<>(APIResponse.FailureResponse("Invalid D2RML document: " + ex.getMessage()), HttpStatus.BAD_REQUEST);
	    	}
    	} catch (Exception ex) {
    		return new ResponseEntity<>(APIResponse.FailureResponse(ex.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
    	}
    	
    }	
	

//	
//	public static void main(String[] args) throws Exception {
//		String s = IOUtils.readFileAsString(new File("D:\\data\\apollonis\\data\\aski\\import.ttl"), "UTF-8");
////		s = convert(s);
////		System.out.println(s);
//		
//		Model model = ModelFactory.createDefaultModel();
//		
//				RDFDataMgr.read(model, new StringReader(s), null, Lang.TURTLE);
//				
//				Writer sw = new StringWriter();
//				
////				RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
//				RDFDataMgr.write(sw, model, RDFFormat.JSONLD) ;
//		
//				System.out.println(sw);
//				
//				Model model2 = ModelFactory.createDefaultModel();
//				
//						RDFDataMgr.read(model2, new StringReader(sw.toString()), null, Lang.JSONLD);
//						
//				        StringWriter sw2 = new StringWriter();
//				        RDFDataMgr.write(sw2, model2, RDFFormat.TURTLE);
//				        
//				        System.out.println(sw2.toString());				
//		
//	}
	


}
