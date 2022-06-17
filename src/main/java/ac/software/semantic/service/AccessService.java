package ac.software.semantic.service;

import java.sql.SQLException;

import ac.software.semantic.model.Dataset;
import ac.software.semantic.payload.EditorItem;
import ac.software.semantic.repository.DatasetRepository;
import com.github.jsonldjava.utils.Obj;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import ac.software.semantic.repository.AccessRepository;
import ac.software.semantic.repository.UserRepository;

import ac.software.semantic.payload.AccessItem;
import ac.software.semantic.model.Access;
import ac.software.semantic.model.AccessType;

@Service
public class AccessService {

    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private AccessRepository accessRepository;

    @Autowired
    private DatasetRepository datasetRepository;

    public AccessItem createAccessById(String creatorId, String validatorId, String collectionId) throws SQLException {
        Optional<Access> isDuplicate = accessRepository.findByCreatorIdAndUserIdAndCollectionId(new ObjectId(creatorId), new ObjectId(validatorId), new ObjectId(collectionId));
        Optional<Dataset> dataset = datasetRepository.findById(new ObjectId(collectionId));
        if (dataset.isPresent() && !isDuplicate.isPresent()) {
            Dataset ds = dataset.get();
            Access newEntry = new Access(creatorId, validatorId, collectionId, ds.getUuid(), AccessType.VALIDATOR);
            accessRepository.save(newEntry);
            return new AccessItem(newEntry);
        }
        return null;
    }

    public AccessItem createAccessByUuid(String creatorId, String validatorId, String collectionUuid){
        Optional<Access> isDuplicate = accessRepository.findByCreatorIdAndUserIdAndCollectionUuid(new ObjectId(creatorId), new ObjectId(validatorId), collectionUuid);
        Optional<Dataset> dataset = datasetRepository.findByUuid(collectionUuid);
        if (dataset.isPresent() && !isDuplicate.isPresent()) {
            Dataset ds = dataset.get();
            Access newEntry = new Access(creatorId, validatorId, ds.getId().toString(), collectionUuid, AccessType.VALIDATOR);
            accessRepository.save(newEntry);
            return new AccessItem(newEntry);
        }
        return null;
    }

    public boolean deleteAccessEntry(String accessId) {
        Long response = accessRepository.deleteById(new ObjectId(accessId));
        if (response > 0) {
            return true;
        }
        else {
            return false;
        }
    }

    public boolean deleteAccessEntryByIds(String editorId, String validatorId, String datasetUuid) {
        Optional<Access> accessEntry = accessRepository.findByCreatorIdAndUserIdAndCollectionUuidAndAccessType(new ObjectId(editorId), new ObjectId(validatorId), datasetUuid, AccessType.VALIDATOR);
        if (accessEntry.isPresent()) {
            Access ac = accessEntry.get();
            ac.toString();
            accessRepository.delete(ac);
            return true;
        }
        else {
            return false;
        }
    }

    public List<AccessItem> getAccessEntries() {
        List<Access> accessList = accessRepository.findAll();
        List<AccessItem> response = accessList.stream()
                .map(access -> new AccessItem(access))
                .collect(Collectors.toList());
        return response;
    }

    public Long removeAccessEntriesByValidatorId(String editorId, String validatorId) {
        Long response = accessRepository.deleteByCreatorIdAndUserId(new ObjectId(editorId), new ObjectId(validatorId));
        return response;
    }


}