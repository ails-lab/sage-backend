package ac.software.semantic.service;

public class Link implements Comparable<Link> {
	private String target;
	private double score;
	
	Link(String target, double score) {
		this.target = target;
		this.score = score;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
	
	public int hashCode() {
		return target.hashCode();
	}
	
	public boolean equals(Object obj) {
		if (!(obj instanceof Link)) {
			return false;
		}
		
		return target.equals(((Link)obj).target);
	}

	@Override
	public int compareTo(Link o) {
		if (score < o.score) {
			return 1;
		} else if (score > o.score) {
			return -1;
		} else {
			return 0;
		}
	}
	
	public String toString() {
		return target + " : " + score;
	}
	
	
}