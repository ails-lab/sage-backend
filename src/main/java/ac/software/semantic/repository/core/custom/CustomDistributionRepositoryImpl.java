package ac.software.semantic.repository.core.custom;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.DistributionDocument;
import ac.software.semantic.model.constants.type.IdentifierType;

@Repository
public class CustomDistributionRepositoryImpl implements CustomDistributionRepository {
	
	@Autowired
	@Qualifier("coreMongoTemplate")
    private MongoTemplate mongoTemplate;
    
	@Override
	public boolean existsSameIdentifier(DistributionDocument object, IdentifierType type) {
		Query query = new Query();

		Criteria cr = Criteria.where("databaseId").is(object.getDatabaseId());
		cr.and("datasetId").is(object.getDatasetId());

		if (type == IdentifierType.IDENTIFIER) {
			cr.and("identifier").is(object.getIdentifier());
		} else {
			return true;
		}
		
		if (object.getId() != null) {
			cr.and("_id").ne(object.getId());
		}

		query.addCriteria(cr);
		
		List<DistributionDocument> res = this.mongoTemplate.find(query, DistributionDocument.class);
		if (res.size() > 0) {
			return true;
		} else {
			return false;
		}
	}


}
