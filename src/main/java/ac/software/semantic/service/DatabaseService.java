package ac.software.semantic.service;

import java.sql.SQLException;
import java.util.Optional;
import org.bson.types.ObjectId;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.Database;
import ac.software.semantic.repository.root.DatabaseRepository;


@Service
public class DatabaseService {

	@Autowired
	private DatabaseRepository dbRepository;

	public boolean createDatabase(String name, String label) throws SQLException {
		
		Optional<Database> db = dbRepository.findByName(name);
		if (db.isPresent()) {
			return false;
		}

		dbRepository.save(new Database(name, label));
		
		return true;
	}

	public Database findDatabase(String name) {
		Optional<Database> db =  dbRepository.findByName(name);
		if (db.isPresent()) {
			return db.get();
		}
		else {
			return null;
		}
	}
}