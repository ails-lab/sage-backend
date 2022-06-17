package ac.software.semantic.model;

import java.util.List;

public class AlignmentDescriptor {

	private GraphDescriptor alignment;
	private GraphDescriptor source;
	private GraphDescriptor target;
	
	
	public AlignmentDescriptor(GraphDescriptor alignment, GraphDescriptor source, GraphDescriptor target) {
		this.alignment = alignment;
		this.source = source;
		this.target = target;
		
	}

	public GraphDescriptor getAlignment() {
		return alignment;
	}

	public void setAlignment(GraphDescriptor alignment) {
		this.alignment = alignment;
	}

	public GraphDescriptor getSource() {
		return source;
	}

	public void setSource(GraphDescriptor source) {
		this.source = source;
	}

	public GraphDescriptor getTarget() {
		return target;
	}

	public void setTarget(GraphDescriptor target) {
		this.target = target;
	}
	
}
