package ac.software.semantic.payload.stats;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class StatisticsValueCount {
	private String uri;
	private String rdfPath;
	
	private Integer count;
	
	private Integer total;
	
	private Integer totalDistinct;
	
	private Integer fresh;
	
	private Integer freshDistinct;
	
	private Integer accepted;
	
	private Integer acceptedDistinct;
	
	private Integer rejected;
	
	private Integer rejectedDistinct;
	
	private Integer added;
	
	private Integer addedDistinct;
	
	private StatisticsValueCount(String rdfPath, String uri) {
		this.rdfPath = rdfPath;
		this.uri = uri;
	}

	public static StatisticsValueCount rdfPathCount(String rdfPath) {
		return new StatisticsValueCount(rdfPath, null);
	}

	public static StatisticsValueCount uriCount(String uri) {
		return new StatisticsValueCount(null, uri);
	}

	public String getRdfPath() {
		return rdfPath;
	}

	public void setRdfPath(String rdfPath) {
		this.rdfPath = rdfPath;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public Integer getTotal() {
		return total;
	}

	public void setTotal(Integer total) {
		if (total == 0) {
			total = null;
		}
		this.total = total;
	}

	public Integer getFresh() {
		return fresh;
	}

	public void setFresh(Integer fresh) {
		if (fresh == 0) {
			fresh = null;
		}

		this.fresh = fresh;
	}

	public Integer getRejected() {
		return rejected;
	}

	public void setRejected(Integer rejected) {
		if (rejected == 0) {
			rejected = null;
		}
		
		this.rejected = rejected;
	}

	public Integer getAccepted() {
		return accepted;
	}

	public void setAccepted(Integer accepted) {
		if (accepted == 0) {
			accepted = null;
		}
		
		this.accepted = accepted;
	}

	public Integer getAdded() {
		return added;
	}

	public void setAdded(Integer added) {
		if (added == 0) {
			added = null;
		}
		
		this.added = added;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		if (count == 0) {
			count = null;
		}		
		
		this.count = count;
	}

	public Integer getTotalDistinct() {
		return totalDistinct;
	}

	public void setTotalDistinct(Integer totalDistinct) {
		this.totalDistinct = totalDistinct;
	}

	public Integer getFreshDistinct() {
		return freshDistinct;
	}

	public void setFreshDistinct(Integer freshDistinct) {
		this.freshDistinct = freshDistinct;
	}

	public Integer getAcceptedDistinct() {
		return acceptedDistinct;
	}

	public void setAcceptedDistinct(Integer acceptedDistinct) {
		this.acceptedDistinct = acceptedDistinct;
	}

	public Integer getRejectedDistinct() {
		return rejectedDistinct;
	}

	public void setRejectedDistinct(Integer rejectedDistinct) {
		this.rejectedDistinct = rejectedDistinct;
	}

	public Integer getAddedDistinct() {
		return addedDistinct;
	}

	public void setAddedDistinct(Integer addedDistinct) {
		this.addedDistinct = addedDistinct;
	}
}
