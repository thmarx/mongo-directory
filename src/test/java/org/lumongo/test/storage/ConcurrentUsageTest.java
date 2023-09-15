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

import java.io.IOException;
import java.util.UUID;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.assertj.core.api.Assertions;
import org.lumongo.storage.lucene.DistributedDirectory;
import org.lumongo.storage.lucene.MongoDirectory;
import org.lumongo.util.TestHelper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ConcurrentUsageTest extends ContainerTest{
	private final String STORAGE_TEST_INDEX = "concurrent";
	private Directory directory;

	private SearcherManager searcherManager;
	
	private IndexWriter writer;
	
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

	private void addDoc(String title, String uid) throws IOException {
		Document doc = new Document();
		
		doc.add(new TextField("title", title, Field.Store.YES));
		doc.add(new StringField("uid", uid, Field.Store.YES));
		Term uidTerm = new Term("uid", uid);

		writer.updateDocument(uidTerm, doc);
	}

	@Test
	public void extern_reader_can_see_changes () throws IOException {
		
		addDoc("lets change the index", UUID.randomUUID().toString());
		writer.commit();
		
		try (
				var readerDirectory = new DistributedDirectory(new MongoDirectory(mongoClient, TestHelper.TEST_DATABASE_NAME, STORAGE_TEST_INDEX, false, true));
				SearcherManager sm = new SearcherManager(readerDirectory, new SearcherFactory());) {
			
			
			Assertions.assertThat(sm.isSearcherCurrent()).isTrue();
			Assertions.assertThat(count(sm)).isEqualTo(1);
			addDoc("lets change the index", UUID.randomUUID().toString());
			writer.commit();
			Assertions.assertThat(count(sm)).isEqualTo(1);
			Assertions.assertThat(sm.isSearcherCurrent()).isFalse();
			
			sm.maybeRefreshBlocking();
			Assertions.assertThat(count(sm)).isEqualTo(2);
			Assertions.assertThat(sm.isSearcherCurrent()).isTrue();
			
			sm.close();
		}
	}
	
	private int count (final SearcherManager sm) throws IOException {
		IndexSearcher is = sm.acquire();
		try {
			return is.getIndexReader().numDocs();
		} finally {
			sm.release(is);
		}
	}
	
}