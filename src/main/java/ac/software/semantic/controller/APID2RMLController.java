package ac.software.semantic.controller;


import io.swagger.v3.oas.annotations.Parameter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLdApi;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;

import ac.software.semantic.config.AppConfiguration.JenaRDF2JSONLD;
import ac.software.semantic.model.constants.type.SerializationType;
import ac.software.semantic.payload.response.APIResponse;
import ac.software.semantic.payload.response.D2RMLValidateResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.D2RMLService;
import edu.ntua.isci.ac.d2rml.model.D2RMLFactory;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLOPVocabulary;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFVocabulary;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "D2RML API")

@RestController
@RequestMapping("/api/d2rml")
public class APID2RMLController {

	@Autowired
	private D2RMLService d2rmlService;
	
	@Autowired
	@Qualifier("d2rml-jsonld-context")
    private Map<String, Object> d2rmlContext;
	
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
	
    @PostMapping(value = "/validate", produces = "application/json")
    public ResponseEntity<APIResponse> validate(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestBody String d2rml) throws Exception {
    	
    	try {
	        
//	        
//	       	Map<String, Object> frame = new HashMap<>();
//	    	//frame.put("@type" , "http://www.w3.org/ns/oa#Annotation");
//	    	frame.put("@context", ((Map)d2rmlContext.get("d2rml-jsonld-context")).get("@context")); // why ????
//
//	        JsonLdOptions options = new JsonLdOptions();
//	        options.setCompactArrays(true);
//	        options.useNamespaces = true ; 
//	        options.setUseNativeTypes(true); 	      
//	        options.setOmitGraph(false);
//	        options.setPruneBlankNodeIdentifiers(true);
//		    
//        	Dataset dataset = DatasetFactory.create();
//	        	
//        	try (InputStream is = new ByteArrayInputStream(d2rml.getBytes())) {
//        		RDFDataMgr.read(dataset, is, Lang.TRIG);
//
//        		Model model = dataset.getDefaultModel();
//	        		
//		        final RDFDataset jsonldDataset = (new JenaRDF2JSONLD()).parse(DatasetFactory.wrap(model).asDatasetGraph());
//		        Object obj = (new JsonLdApi(options)).fromRDF(jsonldDataset, true);
////						    
//		        Map<String, Object> jn = JsonLdProcessor.frame(obj, frame, options);
//	        
//	//		        mapper.writerWithDefaultPrettyPrinter().writeValue(bos, jn);
//		        JsonGenerator jsonGenerator = null;
//				ByteArrayOutputStream jsonbos = new ByteArrayOutputStream();
//				ObjectMapper mapper = new ObjectMapper();
//				
//				JsonFactory jsonFactory = new JsonFactory();				
//				jsonGenerator = jsonFactory.createGenerator(jsonbos, JsonEncoding.UTF8);
//				jsonGenerator = jsonGenerator.useDefaultPrettyPrinter();
//				jsonGenerator.writeStartObject();
//	
//				jsonGenerator.writeFieldName("@context");
//				mapper.setDefaultPrettyPrinter(jsonGenerator.getPrettyPrinter());
//				mapper.writerWithDefaultPrettyPrinter().writeValue(jsonGenerator, jn.get("@context"));
//					
//				jsonGenerator.writeFieldName("@graph");
//				jsonGenerator.writeStartArray();
//				
//				for (Object element : (List)jn.get("@graph")) {
//					mapper.writerWithDefaultPrettyPrinter().writeValue(jsonGenerator, element);
//				}	
//				
//				jsonGenerator.writeEndArray();
//				jsonGenerator.writeEndObject();
//				jsonGenerator.flush();
//				
//				System.out.println(new String(jsonbos.toByteArray()));
//        	}
        	
	        return APIResponse.result(d2rmlService.validate(d2rml)).toResponseEntity();

	        	
	    } catch (Exception ex) {
//	    	ex.printStackTrace();
	    	return APIResponse.FailureResponse("Invalid D2RML document: " + ex.getMessage(), HttpStatus.BAD_REQUEST).toResponseEntity();
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
