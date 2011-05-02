package com.kinisoftware.mongodbtraining;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.hasItem;

import java.net.UnknownHostException;
import java.util.List;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.Mongo;
import com.mongodb.MongoException;


public class TrainingTest {
	
	private Mongo mongo;

	@Before
	public void setUp() throws UnknownHostException, MongoException {
		mongo = new Mongo();
	}
	
	@After
	public void setDown() {
		mongo.close();
	}
	
	@Test
	public void testDatabaseExists() throws Exception {		
		List<String> databaseNames = mongo.getDatabaseNames();
		assertThat(databaseNames, hasItem("test"));
	}
	
	@Test
	public void createCollectionSuccessful() throws Exception {		
		DB testDB = mongo.getDB("test");
		testDB.createCollection("personas",null);
		DBCollection personas = testDB.getCollection("personas");
		BasicDBObject persona = new BasicDBObject();
		persona.append("nombre", "pepe");
		persona.append("edad", 25);
		persona.append("email", "pepe@micorreo.es");
		personas.insert(persona);
		assertThat(testDB.getCollectionNames(), hasItem("personas"));
	}
	
	@Test
	public void createDBReferenceSuccessful() throws Exception {
		DB testDB = mongo.getDB("test");
		testDB.createCollection("direcciones", null);
		BasicDBObject direccion = new BasicDBObject();
		direccion.append("calle", "c/ mi calle");
		direccion.append("CP", "10001");
		direccion.append("localidad", "Ciudad");
		DBCollection direcciones = testDB.getCollection("direcciones");
		direcciones.insert(direccion);
		
		ObjectId direccion_id = (ObjectId) ((BasicDBObject) direcciones.findOne(new BasicDBObject("CP","10001"))).get("_id");
		
		DBRef referenciaADireccion = new DBRef(testDB, "direcciones", direccion_id);
		BasicDBObject personaConDBRefEnDireccion = new BasicDBObject();
		personaConDBRefEnDireccion.append("nombre", "paco");
		personaConDBRefEnDireccion.append("edad", 27);
		personaConDBRefEnDireccion.append("email", "paco@sucorreo.es");
		personaConDBRefEnDireccion.append("direccion", referenciaADireccion);
		
		DBCollection personas = testDB.getCollection("personas");
		personas.insert(personaConDBRefEnDireccion);
		
		DBObject paco = personas.findOne(new BasicDBObject("nombre", "paco"));
		DBRef referenciaDireccionPaco = (DBRef) paco.get("direccion");
		DBObject direccionPaco = referenciaDireccionPaco.fetch();
		assertThat((String) direccionPaco.get("localidad"), is("Ciudad"));
	}
	
	@Test
	public void insertPersonClassSuccessfully() throws Exception {
		Persona persona = new Persona();
		persona.put("nombre", "maria");
		persona.put("edad", 28);
		persona.put("email", "no tiene");
		DB testDB = mongo.getDB("test");
		DBCollection personas = testDB.getCollection("personas");
						
		personas.setObjectClass(Persona.class);
		
		personas.insert(persona);
						
		Persona maria = (Persona) personas.findOne(new BasicDBObject("nombre","maria"));
		assertThat((String) maria.get("email"), is("no tiene"));		
	}
}
