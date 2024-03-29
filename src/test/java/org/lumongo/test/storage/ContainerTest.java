package org.lumongo.test.storage;

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

import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public abstract class ContainerTest {
    	protected MongoDBContainer mongdbContainer;
	protected MongoClient mongoClient;

	@BeforeClass
	public void up() {
		mongdbContainer = new MongoDBContainer(DockerImageName.parse(
				"mongo:6.0.9"
		));
		mongdbContainer.start();

		mongoClient = MongoClients.create(mongdbContainer.getConnectionString());
	}

	@AfterClass
	public void down() {
		mongoClient.close();
		mongdbContainer.stop();
	}
}
