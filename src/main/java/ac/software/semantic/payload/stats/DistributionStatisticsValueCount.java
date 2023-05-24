package ac.software.semantic.payload.stats;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.payload.Distribution;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DistributionStatisticsValueCount {
	private String rdfPath;
	private List<Distribution> total;
	private List<Distribution> fresh;
	private List<Distribution> accepted;
	private List<Distribution> rejected;
	
	public DistributionStatisticsValueCount(String rdfPath) {
		this.rdfPath = rdfPath;
	}

	public String getRdfPath() {
		return rdfPath;
	}

	public void setRdfPath(String rdfPath) {
		this.rdfPath = rdfPath;
	}

	public List<Distribution> getTotal() {
		return total;
	}

	public void setTotal(List<Distribution> total) {
		this.total = total;
	}

	public List<Distribution> getFresh() {
		return fresh;
	}

	public void setFresh(List<Distribution> fresh) {
		this.fresh = fresh;
	}

	public List<Distribution> getAccepted() {
		return accepted;
	}

	public void setAccepted(List<Distribution> accepted) {
		this.accepted = accepted;
	}

	public List<Distribution> getRejected() {
		return rejected;
	}

	public void setRejected(List<Distribution> rejected) {
		this.rejected = rejected;
	}


}
