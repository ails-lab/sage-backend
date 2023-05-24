package ac.software.semantic.model;

import ac.software.semantic.model.state.ProcessState;

public class ProcessStateContainer {

	private ProcessState ps;
	private TripleStoreConfiguration vc;
	private ElasticConfiguration ec;
	
	public ProcessStateContainer(ProcessState ps, TripleStoreConfiguration vc) {
		this.ps = ps;
		this.vc = vc;
	}

	public ProcessStateContainer(ProcessState ps, ElasticConfiguration ec) {
		this.ps = ps;
		this.ec = ec;
	}

	public ProcessState getProcessState() {
		return ps;
	}

	public TripleStoreConfiguration getTripleStoreConfiguration() {
		return vc;
	}

	public ElasticConfiguration getElasticConfiguration() {
		return ec;
	}

}
