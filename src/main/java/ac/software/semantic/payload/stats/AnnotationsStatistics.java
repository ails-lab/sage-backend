package ac.software.semantic.payload.stats;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AnnotationsStatistics {
   	
	private String datasetUri;
//   	private int totalAnnotations;
   	
	private List<StatisticsValueCount> annotatedItems;
   	private List<StatisticsValueCount> annotations;

   	private List<ComplexStatisticsValueCount> mostFrequentAnnotations;
   	
   	private List<DistributionStatisticsValueCount> scoreDistribution;
   	
   	public AnnotationsStatistics(String datasetUri) {
   		this.datasetUri = datasetUri;
   	}
   	
	public String getDatasetUri() {
		return datasetUri;
	}

	public void setDatasetUri(String datasetUri) {
		this.datasetUri = datasetUri;
	}

	public List<StatisticsValueCount> getAnnotatedItems() {
		return annotatedItems;
	}

	public void setAnnotatedItems(List<StatisticsValueCount> annotatedItems) {
		this.annotatedItems = annotatedItems;
	}
	
	public void addAnnotatedItems(StatisticsValueCount annotatedItems) {
		if (this.annotatedItems == null) {
			this.annotatedItems = new ArrayList<>();
		}
		
		this.annotatedItems.add(annotatedItems);
	}


	public List<StatisticsValueCount> getAnnotations() {
		return annotations;
	}

	public void setTotalAnnotations(List<StatisticsValueCount> annotations) {
		this.annotations = annotations;
	}
	
	public void addAnnotations(StatisticsValueCount annotations) {
		if (this.annotations == null) {
			this.annotations = new ArrayList<>();
		}
		
		this.annotations.add(annotations);
	}

	public List<ComplexStatisticsValueCount> getMostFrequentAnnotations() {
		return mostFrequentAnnotations;
	}

	public void setMostFrequentAnnotations(List<ComplexStatisticsValueCount> mostFrequentAnnotations) {
		this.mostFrequentAnnotations = mostFrequentAnnotations;
	}
	
	public void addMostFrequentAnnotations(ComplexStatisticsValueCount mostFrequentAnnotations) {
		if (this.mostFrequentAnnotations == null) {
			this.mostFrequentAnnotations = new ArrayList<>();
		}
		
		this.mostFrequentAnnotations.add(mostFrequentAnnotations);
	}

	public List<DistributionStatisticsValueCount> getScoreDistribution() {
		return scoreDistribution;
	}

	public void setScoreDistribution(List<DistributionStatisticsValueCount> scoreDistribution) {
		this.scoreDistribution = scoreDistribution;
	}
	
	public void addScoreDistribution(DistributionStatisticsValueCount scoreDistribution) {
		if (this.scoreDistribution == null) {
			this.scoreDistribution = new ArrayList<>();
		}
		
		this.scoreDistribution.add(scoreDistribution);
	}



}