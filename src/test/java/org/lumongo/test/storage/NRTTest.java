package org.lumongo.test.storage;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.lumongo.storage.lucene.DistributedDirectory;
import org.lumongo.storage.lucene.MongoDirectory;
import org.lumongo.util.TestHelper;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.NRTCachingDirectory;
import org.assertj.core.api.Assertions;

import static org.testng.AssertJUnit.assertEquals;

public class NRTTest {
	private final String STORAGE_TEST_INDEX = "storageTest";
	private Directory directory;

	private SearcherManager searcherManager;
	
	private IndexWriter writer;
	
	@BeforeClass
	public void cleanDatabaseAndInit() throws Exception {

		MongoClient mongo = TestHelper.getMongo();
		mongo.getDatabase(TestHelper.TEST_DATABASE_NAME).drop();
		directory = new DistributedDirectory(new MongoDirectory(mongo, TestHelper.TEST_DATABASE_NAME, STORAGE_TEST_INDEX, false));

		NRTCachingDirectory cachedFSDir = new NRTCachingDirectory(directory, 5.0, 60.0);
		
		StandardAnalyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(analyzer);

		writer = new IndexWriter(cachedFSDir, config);

		
		searcherManager = new SearcherManager(writer, true, false, new SearcherFactory());
	}

	@AfterClass
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
	public void nrt_test_1 () throws IOException {
		
		
		addDoc("nrt_test_1", "nrtt1");
		
		searcherManager.maybeRefreshBlocking();
		
		IndexSearcher searcher = searcherManager.acquire();
		try {
			
			BooleanQuery.Builder builder = new BooleanQuery.Builder();
			builder.add(new TermQuery(new Term("uid", "nrtt1")), BooleanClause.Occur.MUST);
			
			TopDocs topDocs = searcher.search(builder.build(), Integer.MAX_VALUE);
			
			Assertions.assertThat(topDocs.totalHits.value).isEqualTo(1);
		} finally {
			searcherManager.release(searcher);
		}
		
	}
	
}
