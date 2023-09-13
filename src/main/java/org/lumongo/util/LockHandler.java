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

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockHandler {
	
	private LockIndexer segmentIndexer;
	private ReadWriteLock[] readWriteLock;
	
	public LockHandler() {
		this.segmentIndexer = new LockIndexer(10);
		
		readWriteLock = new ReadWriteLock[segmentIndexer.getSegmentSize()];
		for (int i = 0; i < segmentIndexer.getSegmentSize(); i++) {
			readWriteLock[i] = new ReentrantReadWriteLock();
		}
	}
	
	public ReadWriteLock getLock(String uniqueId) {
		int h = uniqueId.hashCode();
		int index = segmentIndexer.getIndex(h);
		return readWriteLock[index];
	}

	public ReadWriteLock getLock(long uniqueId) {
		int h = Long.hashCode(uniqueId);
		int index = segmentIndexer.getIndex(h);
		return readWriteLock[index];
	}

	public ReadWriteLock getLock(Long uniqueId) {
		int h = uniqueId.hashCode();
		int index = segmentIndexer.getIndex(h);
		return readWriteLock[index];
	}
}
