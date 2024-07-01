package ac.software.semantic.payload.response;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Distribution {
	private int count;
	private double lowerBound;
	private double upperBound;
	private boolean lowerBoundIncluded;
	private boolean upperBoundIncluded;
	
	private Double averageScore;
	
	public int getCount() {
		return count;
	}
	
	public void setCount(int count) {
		this.count = count;
	}
	
	public double getLowerBound() {
		return lowerBound;
	}
	
	public void setLowerBound(double lowerBound) {
		this.lowerBound = lowerBound;
	}
	
	public double getUpperBound() {
		return upperBound;
	}
	
	public void setUpperBound(double upperBound) {
		this.upperBound = upperBound;
	}
	
	public boolean isLowerBoundIncluded() {
		return lowerBoundIncluded;
	}
	
	public void setLowerBoundIncluded(boolean lowerBoundIncluded) {
		this.lowerBoundIncluded = lowerBoundIncluded;
	}
	
	public boolean isUpperBoundIncluded() {
		return upperBoundIncluded;
	}
	
	public void setUpperBoundIncluded(boolean upperBoundIncluded) {
		this.upperBoundIncluded = upperBoundIncluded;
	}

	public Double getAverageScore() {
		return averageScore;
	}

	public void setAverageScore(Double averageScore) {
		this.averageScore = averageScore;
	}
	
}
