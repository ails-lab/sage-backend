package ac.software.semantic.repository.core.custom;

import java.util.List;

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
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.model.User;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.service.lookup.ProjectLookupProperties;

@Repository
public class CustomUserRepositoryImpl implements CustomUserRepository {
	
	@Autowired
	@Qualifier("coreMongoTemplate")
    private MongoTemplate mongoTemplate;

	@Autowired
    @Qualifier("database")
    private Database database;


	@Override
	public boolean existsSameIdentifier(User object, IdentifierType type) { 
		
		Query query = new Query();
		
		Criteria cr = Criteria.where("databaseId").is(object.getDatabaseId());
		
		if (type == IdentifierType.IDENTIFIER) {
			cr.and("identifier").is(object.getIdentifier());
		} else if (type == IdentifierType.EMAIL) {		
			cr.and("email").is(object.getEmail());
		} else {
			return false;
		}
		
		if (object.getId() != null) {
			cr.and("_id").ne(object.getId());
		}

		query.addCriteria(cr);
		
		List<User> res = this.mongoTemplate.find(query, User.class);
		if (res.size() > 0) {
			return true;
		} else {
			return false;
		}
	}

}
