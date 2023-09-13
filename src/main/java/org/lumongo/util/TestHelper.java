package org.lumongo.util;

/*-
 * #%L
 * mongo-directory
 * %%
 * Copyright (C) 2023 Marx-Software
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import java.net.UnknownHostException;

public class TestHelper {

	public static final String MONGO_SERVER_PROPERTY = "mongoServer";
	public static final String TEST_DATABASE_NAME = "lumongoUnitTest";

	public static final String MONGO_SERVER_PROPERTY_DEFAULT = "mongodb://127.0.0.1:27017";

	public static String getMongoServer() {
		String mongoServer = System.getenv("MONGO_SEARCH_CONNECTIONSTRING");
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
