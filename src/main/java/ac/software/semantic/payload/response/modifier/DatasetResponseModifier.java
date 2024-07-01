package ac.software.semantic.payload.response.modifier;

import ac.software.semantic.payload.response.ResponseFieldType;

public class DatasetResponseModifier implements ResponseModifier {

	private ResponseFieldType category;
	private ResponseFieldType scope;
	private ResponseFieldType type;
	private ResponseFieldType typeUri;
	private ResponseFieldType links;
	private ResponseFieldType remoteTripleStore;
	private ResponseFieldType entityDescriptors;
	private ResponseFieldType dates;
//	
	private ResponseFieldType states;
	
	private ResponseFieldType template;
	
	private ResponseFieldType datasets;
	private ResponseFieldType projects;
	
	private ResponseFieldType user;
	private ResponseFieldType mappings;
	private ResponseFieldType rdfFiles;
	private ResponseFieldType userTasks;
	private ResponseFieldType distributions;
	private ResponseFieldType indices;
	private ResponseFieldType prototypes;
	
	public DatasetResponseModifier() {
		category = ResponseFieldType.EXPAND;
		scope = ResponseFieldType.EXPAND;
		type = ResponseFieldType.EXPAND;
		typeUri = ResponseFieldType.EXPAND;
		links = ResponseFieldType.EXPAND;
		remoteTripleStore = ResponseFieldType.EXPAND;
		entityDescriptors = ResponseFieldType.EXPAND;
		
		states = ResponseFieldType.EXPAND;
		template = ResponseFieldType.EXPAND;
		datasets = ResponseFieldType.EXPAND;
		projects = ResponseFieldType.EXPAND;
		dates = ResponseFieldType.EXPAND;
		
		user = ResponseFieldType.IGNORE;
		mappings = ResponseFieldType.IGNORE;
		rdfFiles = ResponseFieldType.IGNORE;
		userTasks = ResponseFieldType.IGNORE;
		distributions = ResponseFieldType.IGNORE;
		indices = ResponseFieldType.IGNORE;
		prototypes = ResponseFieldType.IGNORE;
	}
	
	public static DatasetResponseModifier baseModifier() {
		DatasetResponseModifier modifier = new DatasetResponseModifier();
		modifier.setCategory(ResponseFieldType.IGNORE);
		modifier.setScope(ResponseFieldType.IGNORE);
		modifier.setType(ResponseFieldType.IGNORE);
		modifier.setTypeUri(ResponseFieldType.IGNORE);
		modifier.setLinks(ResponseFieldType.IGNORE);
		modifier.setStates(ResponseFieldType.IGNORE);
		modifier.setTemplate(ResponseFieldType.IGNORE);
		modifier.setDatasets(ResponseFieldType.IGNORE);
		modifier.setProjects(ResponseFieldType.IGNORE);
		
		return modifier;
	}
	
	public ResponseFieldType getUser() {
		return user;
	}

	public void setUser(ResponseFieldType validatorId) {
		this.user = validatorId;
	}

	public ResponseFieldType getMappings() {
		return mappings;
	}

	public void setMappings(ResponseFieldType mappings) {
		this.mappings = mappings;
	}

	public ResponseFieldType getUserTasks() {
		return userTasks;
	}

	public void setUserTasks(ResponseFieldType userTasks) {
		this.userTasks = userTasks;
	}

	public ResponseFieldType getDistributions() {
		return distributions;
	}

	public void setDistributions(ResponseFieldType distributions) {
		this.distributions = distributions;
	}

	public ResponseFieldType getIndices() {
		return indices;
	}

	public void setIndices(ResponseFieldType indices) {
		this.indices = indices;
	}

	public ResponseFieldType getRdfFiles() {
		return rdfFiles;
	}

	public void setRdfFiles(ResponseFieldType rdfFiles) {
		this.rdfFiles = rdfFiles;
	}

	public ResponseFieldType getCategory() {
		return category;
	}

	public void setCategory(ResponseFieldType category) {
		this.category = category;
	}

	public ResponseFieldType getScope() {
		return scope;
	}

	public void setScope(ResponseFieldType scope) {
		this.scope = scope;
	}

	public ResponseFieldType getType() {
		return type;
	}

	public void setType(ResponseFieldType type) {
		this.type = type;
	}

	public ResponseFieldType getTypeUri() {
		return typeUri;
	}

	public void setTypeUri(ResponseFieldType typeUri) {
		this.typeUri = typeUri;
	}

	public ResponseFieldType getLinks() {
		return links;
	}

	public void setLinks(ResponseFieldType links) {
		this.links = links;
	}

	public ResponseFieldType getStates() {
		return states;
	}

	public void setStates(ResponseFieldType states) {
		this.states = states;
	}

	public ResponseFieldType getRemoteTripleStore() {
		return remoteTripleStore;
	}

	public void setRemoteTripleStore(ResponseFieldType remoteTripleStore) {
		this.remoteTripleStore = remoteTripleStore;
	}

	public ResponseFieldType getTemplate() {
		return template;
	}

	public void setTemplate(ResponseFieldType template) {
		this.template = template;
	}

	public ResponseFieldType getDatasets() {
		return datasets;
	}

	public void setDatasets(ResponseFieldType datasets) {
		this.datasets = datasets;
	}

	public ResponseFieldType getEntityDescriptors() {
		return entityDescriptors;
	}

	public void setEntityDescriptors(ResponseFieldType entityDescriptors) {
		this.entityDescriptors = entityDescriptors;
	}

	public ResponseFieldType getDates() {
		return dates;
	}

	public void setDates(ResponseFieldType dates) {
		this.dates = dates;
	}

	public ResponseFieldType getProjects() {
		return projects;
	}

	public void setProjects(ResponseFieldType projects) {
		this.projects = projects;
	}

	public ResponseFieldType getPrototypes() {
		return prototypes;
	}

	public void setPrototypes(ResponseFieldType prototypes) {
		this.prototypes = prototypes;
	}
}
