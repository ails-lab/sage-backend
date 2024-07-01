package ac.software.semantic.repository.core.custom;

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
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.IndexDocument;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.service.lookup.IndexLookupProperties;
import ac.software.semantic.service.lookup.MappingLookupProperties;

@Repository
public class CustomIndexRepositoryImpl implements CustomIndexRepository {
	
	@Autowired
	@Qualifier("coreMongoTemplate")
    private MongoTemplate mongoTemplate;
	
	@Autowired
    @Qualifier("database")
    private Database database;
    
//	public boolean existsSameIdentifier(MappingDocument object) {
//		
//		Query query = new Query();
//
//		Criteria cr = Criteria.where("datasetId").is(object.getDatasetId());
//		cr.and("identifier").is(object.getIdentifier());
//		
//		if (object.getId() != null) {
//			cr.and("_id").ne(object.getId());
//		}
//		
//		query.addCriteria(cr);
//		
//		List<MappingDocument> res = this.mongoTemplate.find(query, MappingDocument.class);
//		if (res.size() > 0) {
//			return true;
//		} else {
//			return false;
//		}
//	}

	@Override
	public List<IndexDocument> find(ObjectId userId, List<Dataset> dataset, IndexLookupProperties lp, ObjectId databaseId) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, dataset, lp, databaseId));
		query.with(new Sort(Sort.Direction.ASC, "group"));
		query.with(new Sort(Sort.Direction.ASC, "order"));
		
		return this.mongoTemplate.find(query, IndexDocument.class);
	}

	@Override
	public Page<IndexDocument> find(ObjectId userId, List<Dataset> dataset, IndexLookupProperties lp, ObjectId databaseId, Pageable page) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, dataset, lp, databaseId)).with(page);
		query.with(new Sort(Sort.Direction.ASC, "group"));
		query.with(new Sort(Sort.Direction.ASC, "order"));
		
		List<IndexDocument> list = mongoTemplate.find(query, IndexDocument.class);
        return PageableExecutionUtils.getPage(list, page, () -> mongoTemplate.count(query, IndexDocument.class));

	}

	private Criteria buildCriteria(ObjectId userId, List<Dataset> dataset, IndexLookupProperties lp, ObjectId databaseId) {
		
		Criteria cr = Criteria.where("databaseId").is(databaseId);
		
		if (userId != null) {
			cr.and("userId").is(userId);
		}
		
		if (dataset != null) {
			cr.and("datasetId").in(dataset.stream().map(p -> p.getId()).collect(Collectors.toList()).toArray());
		}
		
		if (lp != null) {
			if (lp.getElasticConfigurationId() != null) {
				cr.and("elasticConfigurationId").in(lp.getElasticConfigurationId());
			}
		}
		
		return cr;
	}

}
