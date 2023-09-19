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
import com.github.javafaker.Faker;
import com.google.common.util.concurrent.AtomicDouble;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.lumongo.storage.lucene.DistributedDirectory;
import org.lumongo.storage.lucene.MongoDirectory;
import org.lumongo.util.TestHelper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LargeIndexTest extends ContainerTest {

	private final String STORAGE_TEST_INDEX = "storageTest";
	private Directory directory;

	private SearcherManager searcherManager;

	private IndexWriter writer;
	
	private Faker faker = new Faker();

	@BeforeMethod
	public void cleanDatabaseAndInit() throws Exception {

		mongoClient.getDatabase(TestHelper.TEST_DATABASE_NAME).drop();
		directory = new DistributedDirectory(new MongoDirectory(mongoClient, TestHelper.TEST_DATABASE_NAME, STORAGE_TEST_INDEX));

		NRTCachingDirectory cachedFSDir = new NRTCachingDirectory(directory, 5.0, 60.0);

		StandardAnalyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(analyzer);

		writer = new IndexWriter(cachedFSDir, config);

		searcherManager = new SearcherManager(writer, true, false, new SearcherFactory());
	}

	@AfterMethod
	public void closeDirectory() throws Exception {
		writer.close();
		directory.close();
	}

	private void addDoc(Map<String, Object> fields) throws IOException {
		Document doc = new Document();

		doc.add(new StringField("uid", UUID.randomUUID().toString(), Field.Store.YES));

		fields.forEach((key, value) -> {
			if (value instanceof String svalue) {
				doc.add(new TextField(key, svalue, Field.Store.YES));
			} else if (value instanceof Integer ivalue) {
				doc.add(new IntPoint(key, ivalue));
				doc.add(new StoredField(key, ivalue));
			} 
			
		});

		writer.addDocument(doc);
	}

	@Test
	public void test_large_index() throws IOException {

		long before = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			addDoc(Map.of(
					"name", faker.name().fullName(),
					"country", faker.country().name(),
					"title", faker.lorem().paragraph(5),
					"age", ThreadLocalRandom.current().nextInt(),
					"hour", ThreadLocalRandom.current().nextInt()
			));
			if (i % 50 == 0) {
				System.out.println("index: " + i);
			}
		}
		long after = System.currentTimeMillis();
		System.out.println("took: " + (after - before) + "ms");
		
		writer.close();
		
		CheckIndex check = new CheckIndex(directory);
		
		AtomicDouble indexSize = new AtomicDouble(0);
		check.checkIndex().segmentInfos.forEach((segmentInfo) -> {
			indexSize.addAndGet(segmentInfo.sizeMB);
			System.out.println(segmentInfo.name + " - " + segmentInfo.sizeMB + " MB");
		});
		System.out.println("indexSize: " + indexSize.get() + " MB");
	}

}
