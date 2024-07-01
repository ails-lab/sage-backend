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

import ac.software.semantic.model.ComparatorDocument;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.service.lookup.ComparatorLookupProperties;

@Repository
public class CustomComparatorRepositoryImpl implements CustomComparatorRepository {
	
	@Autowired
	@Qualifier("coreMongoTemplate")
    private MongoTemplate mongoTemplate;
	
	@Autowired
    @Qualifier("database")
    private Database database;
    
	public List<ComparatorDocument> find(ObjectId userId, List<Dataset> dataset, ComparatorLookupProperties lp, ObjectId databaseId) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, dataset, lp, databaseId));
		query.with(new Sort(Sort.Direction.ASC, "name"));
		
		return this.mongoTemplate.find(query, ComparatorDocument.class);
	}

	@Override
	public Page<ComparatorDocument> find(ObjectId userId, List<Dataset> dataset, ComparatorLookupProperties lp, ObjectId databaseId, Pageable page) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, dataset, lp, databaseId)).with(page);
		query.with(new Sort(Sort.Direction.ASC, "name"));
		
		List<ComparatorDocument> list = mongoTemplate.find(query, ComparatorDocument.class);
        return PageableExecutionUtils.getPage(list, page, () -> mongoTemplate.count(query, ComparatorDocument.class));

	}

	private Criteria buildCriteria(ObjectId userId, List<Dataset> dataset, ComparatorLookupProperties lp, ObjectId databaseId) {
		
		Criteria cr = Criteria.where("databaseId").is(databaseId);
		
		if (userId != null) {
			cr.and("userId").is(userId);
		}
		
		if (dataset != null) {
			cr.and("datasetId").in(dataset.stream().map(p -> p.getId()).collect(Collectors.toList()).toArray());
		}
		
		if (lp != null) {
//			if (lp.getPrototypeType() != null) {
//				cr.and("type").is(lp.getPrototypeType());
//			}
		}
		
		return cr;
	}
	
	@Override
	public boolean existsSameIdentifier(ComparatorDocument object, IdentifierType type) {
		
		Query query = new Query();
		
		Criteria cr = Criteria.where("databaseId").is(object.getDatabaseId());
		
		if (type == IdentifierType.IDENTIFIER) {
			cr.and("identifier").is(object.getIdentifier());
		} else {
			return false;
		}
		
		if (object.getId() != null) {
			cr.and("_id").ne(object.getId());
		}

		query.addCriteria(cr);
		
		List<ComparatorDocument> res = this.mongoTemplate.find(query, ComparatorDocument.class);
		if (res.size() > 0) {
			return true;
		} else {
			return false;
		}
	}

}
