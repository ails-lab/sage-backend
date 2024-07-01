package ac.software.semantic.model;

import ac.software.semantic.model.state.ProcessState;

public class ProcessStateContainer<T extends ProcessState> {

	private T ps;
	private TripleStoreConfiguration vc;
	private ElasticConfiguration ec;
	
	public ProcessStateContainer(T ps, TripleStoreConfiguration vc) {
		this.ps = ps;
		this.vc = vc;
	}

	public ProcessStateContainer(T ps, ElasticConfiguration ec) {
		this.ps = ps;
		this.ec = ec;
	}

	public T getProcessState() {
		return ps;
	}

	public TripleStoreConfiguration getTripleStoreConfiguration() {
		return vc;
	}

	public ElasticConfiguration getElasticConfiguration() {
		return ec;
	}

}
