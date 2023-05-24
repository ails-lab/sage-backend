package ac.software.semantic.service;

import java.io.StringReader;
import java.util.Map;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.constants.MappingType;
import ac.software.semantic.vocs.SEMRVocabulary;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.parser.Parser;
import edu.ntua.isci.ac.d2rml.vocabulary.D2RMLVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.R2RMLVocabulary;


@Service
public class D2RMLService {

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	public D2RMLModel prepare(MappingDocument mapping, ac.software.semantic.model.Dataset dataset, String baseUri, Map<String, Object> params) throws Exception {

		Dataset ds = DatasetFactory.create();

		
		try (StringReader sr = new StringReader(mapping.getFileContents())) {
			RDFDataMgr.read(ds, sr, baseUri, Lang.TRIG);
		}
		
		Model model = ds.getDefaultModel();
		
		UpdateRequest ur = UpdateFactory.create();

		// delete graph maps
		ur.add("DELETE { ?subject <" + R2RMLVocabulary.graph + "> ?object } WHERE { ?subject <" + R2RMLVocabulary.graph + "> ?object }");
		ur.add("DELETE { ?subject <" + R2RMLVocabulary.graphMap + "> ?object } WHERE { ?subject <" + R2RMLVocabulary.graphMap + "> ?object }"); // possibly dangling nodes. may be a problem

		if (mapping.getType() == MappingType.HEADER) {
			Parser.expand(model);
			
			// add subject to metadata mapping
			ur.add("DELETE { ?subject ?prop  ?propValue } " + 
				   "INSERT { ?subject <" + R2RMLVocabulary.constant + "> <" + resourceVocabulary.getDatasetAsResource(dataset.getUuid()) + ">  } " +
			       "WHERE { ?dataset <" + D2RMLVocabulary.triplesMap + ">/<" + R2RMLVocabulary.subjectMap + "> ?subject . ?subject ?prop ?propValue . " + 
			       "VALUES ?prop { <" + R2RMLVocabulary.constant + "> <" + R2RMLVocabulary.column + "> <" + R2RMLVocabulary.template + "> } . FILTER NOT EXISTS { ?root ?property ?dataset }  }");
		}
		
		UpdateAction.execute(ur, model);
		
		D2RMLModel res = new Parser().extractRMLMapping(ds.getDefaultModel(), params);
		
		res.setParseParams(params);
		
		res.setDataset(ds);
				
		return res;
	}	

}
