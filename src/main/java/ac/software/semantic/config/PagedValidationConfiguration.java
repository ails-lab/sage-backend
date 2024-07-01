package ac.software.semantic.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import ac.software.semantic.model.PagedValidationOption;
import ac.software.semantic.vocs.SOAVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.OAVocabulary;

@Configuration
public class PagedValidationConfiguration {
	
	@Bean(name = "paged-validations")
	public Map<String, PagedValidationOption> getPagedValidationOptions() {
		Map<String, PagedValidationOption> res = new HashMap<>();

		String byValueFrequencyData = 
//			"GRAPH <{@@PAV_AS_PROPERTY@@}> { " +
            "  ?v a <" + OAVocabulary.Annotation + "> ; " + 
		    "     <" + OAVocabulary.hasTarget + "> ?r . " + 
               "{@@PAV_GENERATORS@@} " +
            "  ?r <" + SOAVocabulary.onProperty + "> \"{@@PAV_ON_PROPERTY_STRING@@}\" ; " + 
		    "     <" + SOAVocabulary.onValue + "> ?value ; " + 
            "     <" + OAVocabulary.hasSource + "> ?s . " 
//		    " }"
            ;

		String byValueFrequencyQuery = 
	            "SELECT ?value ?valueCount " + 
   			    "{@@PAV_DATASET_URI@@} " + 
//   	            "FROM NAMED <{@@PAV_AS_PROPERTY@@}> " +
				"{@@PAV_AS_PROPERTY_FROM@@} " +
	            "WHERE { " +
//				"  SELECT ?value (count(*) AS ?valueCount)" +
				"  SELECT ?value (count(distinct ?s) AS ?valueCount)" +
		        "  WHERE { " + 
		        "    ?s {@@PAV_ON_PROPERTY_STRING@@} ?value " + 
			         "{@@DATA_QUERY_PART@@} " +
			    "    FILTER (isLiteral(?value)) " +		         
			    "  } " +
				"  GROUP BY ?value " + 
				"  ORDER BY {@@PAV_SORTING_VAL:CNT@@}(?valueCount) ?value } "; 
		
		String byValueFrequencyAll = byValueFrequencyQuery.replaceAll("\\{@@DATA_QUERY_PART@@\\}", byValueFrequencyData);
		String byPropertyValueAnnotated = byValueFrequencyQuery.replaceAll("\\{@@DATA_QUERY_PART@@\\}", " FILTER EXISTS { " + byValueFrequencyData + " }  ");
		String byPropertyValueNonAnnotated = byValueFrequencyQuery.replaceAll("\\{@@DATA_QUERY_PART@@\\}", " FILTER NOT EXISTS { " + byValueFrequencyData + " }  ");
		
		res.put("PAV-VAL:CNT", new PagedValidationOption("PAV-VAL:CNT", Arrays.asList(new String[] { "property value count" }), byValueFrequencyAll, byPropertyValueAnnotated, byPropertyValueNonAnnotated));
		
		String byAvgScoreAndValueFrequencyDataAnnotated = 
//				"GRAPH <{@@PAV_AS_PROPERTY@@}> { " +
	            "  ?v a <" + OAVocabulary.Annotation + "> ; " +
			    "     <" + OAVocabulary.hasTarget + "> ?r . " +
	            "  OPTIONAL { ?v <" + SOAVocabulary.score + ">  ?score } " +
	               "{@@PAV_GENERATORS@@} " +
	            "  ?r <" + SOAVocabulary.onProperty + "> \"{@@PAV_ON_PROPERTY_STRING@@}\" ; " + 
			    "     <" + SOAVocabulary.onValue + "> ?value ; " + 
	            "     <" + OAVocabulary.hasSource + "> ?s . " 
//			    " }"
	            ;

		String byAvgScoreAndValueFrequencyDataNotAnnotated = 
//				"GRAPH <{@@PAV_AS_PROPERTY@@}> { " +
	            "  ?v a <" + OAVocabulary.Annotation + "> ; " +
			    "     <" + OAVocabulary.hasTarget + "> ?r . " + 
	               "{@@PAV_GENERATORS@@} " +
	            "  ?r <" + SOAVocabulary.onProperty + "> \"{@@PAV_ON_PROPERTY_STRING@@}\" ; " + 
			    "     <" + SOAVocabulary.onValue + "> ?value ; " + 
	            "     <" + OAVocabulary.hasSource + "> ?s . " 
//			    " }"
	            ;

		String byAvgScoreAndValueFrequencyQueryAnnotated = 
				"SELECT ?value ?valueCount " + 
			    "{@@PAV_DATASET_URI@@} " + 
//	            "FROM NAMED <{@@PAV_AS_PROPERTY@@}> " +
				"{@@PAV_AS_PROPERTY_FROM@@} " +
  		        "WHERE { " +
				"SELECT ?value (count(distinct ?s) AS ?valueCount) (avg(?score) AS ?avgScore) " +
		        "  WHERE { " + 
		        "    ?s {@@PAV_ON_PROPERTY_STRING@@} ?value " + 
			         "{@@DATA_QUERY_PART@@} " +
			    "    FILTER (isLiteral(?value)) " +		         
			    "  } " +
				"  GROUP BY ?value " + 
				"  ORDER BY {@@PAV_SORTING_SCR:AVG@@}(?avgScore) {@@PAV_SORTING_VAL:CNT@@}(?valueCount) ?value } "; 

		String byAvgScoreAndValueFrequencyQueryNotAnnotated = 
				"SELECT ?value ?valueCount " + 
 			    "{@@PAV_DATASET_URI@@} " + 
//	            "FROM NAMED <{@@PAV_AS_PROPERTY@@}> " +
				"{@@PAV_AS_PROPERTY_FROM@@}> " +
	            "WHERE { " +
//				"SELECT ?value (count(*) AS ?valueCount) " +
                "SELECT ?value (count(distinct ?s) AS ?valueCount) " +
		        "  WHERE { " + 
		        "    ?s {@@PAV_ON_PROPERTY_STRING@@} ?value " + 
			         "{@@DATA_QUERY_PART@@} " +
			    "    FILTER (isLiteral(?value)) " +		         
			    "  } " +
				"  GROUP BY ?value " + 
				"  ORDER BY {@@PAV_SORTING_VAL:CNT@@}(?valueCount) ?value } "; 

			String byAvgScoreAndValueFrequencyAll = null;
			String byAvgScoreAndValueFrequencyAnnotated = byAvgScoreAndValueFrequencyQueryAnnotated.replaceAll("\\{@@DATA_QUERY_PART@@\\}", " { " + byAvgScoreAndValueFrequencyDataAnnotated + " }  ");
			String byAvgScoreAndValueFrequencyNonAnnotated = byAvgScoreAndValueFrequencyQueryNotAnnotated.replaceAll("\\{@@DATA_QUERY_PART@@\\}", " FILTER NOT EXISTS { " + byAvgScoreAndValueFrequencyDataNotAnnotated + " }  ");
			
			res.put("PAV-SCR:AVG-VAL:CNT", new PagedValidationOption("PAV-SCR:AVG-VAL:CNT", Arrays.asList(new String[] { "average annotation score", "property value count" }), byAvgScoreAndValueFrequencyAll, byAvgScoreAndValueFrequencyAnnotated, byAvgScoreAndValueFrequencyNonAnnotated));
			
		return res;
	}

}
