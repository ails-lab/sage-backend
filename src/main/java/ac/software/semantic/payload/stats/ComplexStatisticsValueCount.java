package ac.software.semantic.payload.stats;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ComplexStatisticsValueCount {
	private String rdfPath;
	private List<StatisticsValueCount> total;
	private List<StatisticsValueCount> fresh;
	private List<StatisticsValueCount> accepted;
	private List<StatisticsValueCount> rejected;
	
	public ComplexStatisticsValueCount(String rdfPath) {
		this.rdfPath = rdfPath;
	}

	public String getRdfPath() {
		return rdfPath;
	}

	public void setRdfPath(String rdfPath) {
		this.rdfPath = rdfPath;
	}

	public List<StatisticsValueCount> getTotal() {
		return total;
	}

	public void setTotal(List<StatisticsValueCount> total) {
		this.total = total;
	}

	public List<StatisticsValueCount> getFresh() {
		return fresh;
	}

	public void setFresh(List<StatisticsValueCount> fresh) {
		this.fresh = fresh;
	}

	public List<StatisticsValueCount> getAccepted() {
		return accepted;
	}

	public void setAccepted(List<StatisticsValueCount> accepted) {
		this.accepted = accepted;
	}

	public List<StatisticsValueCount> getRejected() {
		return rejected;
	}

	public void setRejected(List<StatisticsValueCount> rejected) {
		this.rejected = rejected;
	}


}
