package org.lumongo.storage.constants;

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

public interface MongoConstants {
	public static interface StandardDBs {
		public static final String ADMIN = "admin";
	}

	public static interface StandardFields {
		public static final String _ID = "_id";
	}

	public static interface Operators {
		public static final String LT = "$lt";
		public static final String GT = "$gt";
		public static final String AND = "$and";
		public static final String OR = "$or";
		public static final String NOR = "$nor";
		public static final String NOT = "$not";
		public static final String SET = "$set";
		public static final String INC = "$inc";
		public static final String UNSET = "$unset";
		public static final String ADD_SET = "$addToSet";
		public static final String EACH = "$each";
		public static final String EXISTS = "$exists";
		public static final String SEARCH_HIT = "$";
	}

	public static interface Commands {
		public static final String ENABLE_SHARDING = "enablesharding";
		public static final String SHARD_COLLECTION = "shardcollection";
		public static final String SHARD_KEY = "key";
	}

	public static class Functions {
		public static int BUFFER_SIZE = 1024;

		public static String createNameSpace(String databaseName, String collectionName) {
			return databaseName + "." + collectionName;
		}
	}
}
