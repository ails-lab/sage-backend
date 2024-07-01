package ac.software.semantic.repository.core.custom;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.support.PageableExecutionUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.constants.type.DatasetScope;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.service.lookup.DatasetLookupProperties;
import io.jsonwebtoken.lang.Collections;

@Repository
public class CustomDatasetRepositoryImpl implements CustomDatasetRepository {
	
	@Autowired
	@Qualifier("coreMongoTemplate")
    private MongoTemplate mongoTemplate;
	
	@Autowired
    @Qualifier("database")
    private Database database;
    
    private static DatasetState[] ps = new DatasetState[] { DatasetState.PUBLISHED, DatasetState.PUBLISHED_PRIVATE, DatasetState.PUBLISHED_PUBLIC } ;
    
	public List<Dataset> find(ObjectId userId, List<ProjectDocument> project, DatasetLookupProperties lp, ObjectId databaseId, Collection<ObjectId> tripleStoreId) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, project, lp, databaseId, tripleStoreId));
		if (lp != null && lp.getSortByFields() != null) {
			for (String s : lp.getSortByFields()) {
				if (s.startsWith("-")) {
					query.with(new Sort(Sort.Direction.DESC, s.substring(1)));
				} else {
					query.with(new Sort(Sort.Direction.ASC, s));
				}
			}
		} else {
			query.with(new Sort(Sort.Direction.ASC, "name"));	
		}
		
		return this.mongoTemplate.find(query, Dataset.class);
	}
	
	public Page<Dataset> find(ObjectId userId, List<ProjectDocument> project, DatasetLookupProperties lp, ObjectId databaseId, Collection<ObjectId> tripleStoreId, Pageable page) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, project, lp, databaseId, tripleStoreId)).with(page);
		if (lp != null && lp.getSortByFields() != null) {
			for (String s : lp.getSortByFields()) {
				if (s.startsWith("-")) {
					query.with(new Sort(Sort.Direction.DESC, s.substring(1)));
				} else {
					query.with(new Sort(Sort.Direction.ASC, s));
				}
			}
		} else {
			query.with(new Sort(Sort.Direction.ASC, "name"));	
		}
		
		List<Dataset> list = mongoTemplate.find(query, Dataset.class);
        return PageableExecutionUtils.getPage(list, page, () -> mongoTemplate.count(query, Dataset.class));
	}
	
	private Criteria buildCriteria(ObjectId userId, List<ProjectDocument> project, DatasetLookupProperties lp, ObjectId databaseId, Collection<ObjectId> tripleStoreId) {
		
		
		
//		Criteria cr = Criteria.where("databaseId").is(databaseId);

//		if (userId != null) {
//			cr.and("userId").is(userId);
//		}
//
//		if (project != null) {
//			cr.and("projectId").is(project.getId());
//		}
//
//		if (lp != null) {
//			if (lp.getUserIdNot() != null) {
//				cr.and("userId").ne(lp.getUserIdNot());
//			}
//		
//			if (lp.getDatasetType() != null) {
//				cr.and("type").is(lp.getDatasetType());
//			}
//			
//			if (lp.getDatasetScope() != null) {
//				cr.and("scope").in(lp.getDatasetScope());
//			}
//			
//			if (lp.getOnlyPublished() != null && lp.getOnlyPublished()) {
//				cr.and("publish").elemMatch(Criteria.where("databaseConfigurationId").in(tripleStoreId).and("publishState").in(ps));
//				cr.an
//			}
//			
//			if (lp.getPublik() != null) {
//				cr.and("publik").is(lp.getPublik());
//			}
//		}
		
		Criteria cr = new Criteria();
		
		List<Criteria> conditions = new ArrayList<>();
		
		conditions.add(Criteria.where("databaseId").is(databaseId));
		
		if (userId != null) {
			conditions.add(Criteria.where("userId").is(userId));
		}

		if (project != null) {
			conditions.add(Criteria.where("projectId").in(project.stream().map(p -> p.getId()).collect(Collectors.toList()).toArray()));
		}

		if (lp != null) {
			if (lp.getUserIdNot() != null) {
				conditions.add(Criteria.where("userId").ne(lp.getUserIdNot()));
			}
		
			if (lp.getDatasetType() != null) {
				conditions.add(Criteria.where("type").is(lp.getDatasetType()));
			}
			
			if (lp.getDatasetScope() != null) {
				conditions.add(Criteria.where("scope").in(lp.getDatasetScope()));
			}
			
			if (lp.getOnlyPublished() != null && lp.getOnlyPublished()) {
				
				Criteria[] or = new Criteria[2];
				
				or[0] = Criteria.where("publish").elemMatch(Criteria.where("databaseConfigurationId").in(tripleStoreId).and("publishState").in(ps));
				or[1] = Criteria.where("scope").in(Arrays.asList(new DatasetScope[] { DatasetScope.SHACL, DatasetScope.D2RML, DatasetScope.ANNOTATOR })); // not publishable
						
				conditions.add(cr);
				
			}
			
			if (lp.getPublik() != null) {
				conditions.add(Criteria.where("publik").is(lp.getPublik()));
			}
		}
		
		cr.andOperator(conditions.toArray(new Criteria[] {}));
		
		return cr;
	}
	
	public boolean existsSameIdentifier(Dataset object, IdentifierType type) {
		
//    	if (object.getIdentifier().equals(database.getName())) {
//    		return true;
//    	}
    	
		Query query = new Query();

		Criteria cr = Criteria.where("databaseId").is(object.getDatabaseId());
		
		if (type == IdentifierType.IDENTIFIER) {
			cr.and("identifier").is(object.getIdentifier());
		} else {
			return true;
		}
		
		if (object.getId() != null) {
			cr.and("_id").ne(object.getId());
		}
		
		if (object.getProjectId() != null) { // should be only one!
			cr.and("projectId").in(object.getProjectId());
		} else {
			cr.and("projectId").exists(false);
		}
		
		query.addCriteria(cr);
		
		List<Dataset> res = this.mongoTemplate.find(query, Dataset.class);
		if (res.size() > 0) {
			return true;
		} else {
			return false;
		}
	}


}
