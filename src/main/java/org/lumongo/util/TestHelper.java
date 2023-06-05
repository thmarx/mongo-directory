package org.lumongo.util;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import java.net.UnknownHostException;

public class TestHelper {

	public static final String MONGO_SERVER_PROPERTY = "mongoServer";
	public static final String TEST_DATABASE_NAME = "lumongoUnitTest";

	public static final String MONGO_SERVER_PROPERTY_DEFAULT = "mongodb://127.0.0.1:27017";

	public static String getMongoServer() {
		String mongoServer = System.getProperty(MONGO_SERVER_PROPERTY);
		if (mongoServer == null) {
			return MONGO_SERVER_PROPERTY_DEFAULT;
		}
		return mongoServer;
	}

	public static MongoClient getMongo() throws UnknownHostException, MongoException {
		return MongoClients.create(getMongoServer());
	}

	public static MongoClient getClusteredMongo() throws UnknownHostException, MongoException {
		System.out.println(System.getProperty("atlas_cluster"));
		return MongoClients.create((System.getProperty("atlas_cluster")));
	}
}
