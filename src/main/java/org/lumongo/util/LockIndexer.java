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

public final class LockIndexer {
	
	private static final int BIT_SIZE = 32;
	private final int segmentShift;
	private final int segmentMask;
	private final int segmentSize;
	private final int segmentBits;
	private int segmentFactor;
	
	public LockIndexer(int segmentBits) {
		this(segmentBits, 2, 0);
	}
	
	public LockIndexer(int segmentBits, int segmentFactor) {
		this(segmentBits, segmentFactor, 0);
	}
	
	public LockIndexer(int segmentBits, int segmentFactor, int segmentShift) {
		if (segmentBits + segmentShift > BIT_SIZE) {
			throw new IllegalArgumentException("Segment bits + segment shift cannot be greater than 32");
		}
		
		this.segmentBits = segmentBits;
		this.segmentShift = segmentShift;
		this.segmentSize = (1 << segmentBits);
		this.segmentMask = segmentSize - 1;
		this.segmentFactor = segmentFactor;
	}
	
	public int getSegmentSize() {
		return segmentSize;
	}
	
	protected int getSegmentShift() {
		return segmentShift;
	}
	
	protected int getSegmentMask() {
		return segmentMask;
	}
	
	public int getIndex(int hash) {
		return ((rehash(hash) >>> segmentShift) & segmentMask);
	}
	
	public int getElementsPerSegment() {
		return segmentSize * segmentFactor;
	}
	
	public boolean isLastSegment() {
		return ((segmentShift + segmentBits) == BIT_SIZE);
	}
	
	private static int rehash(int h) {
		
		// Spread bits to regularize both segment and index locations,
		// using variant of single-word Wang/Jenkins hash.
		h += (h << 15) ^ 0xffffcd7d;
		h ^= (h >>> 10);
		h += (h << 3);
		h ^= (h >>> 6);
		h += (h << 2) + (h << 14);
		
		return h ^ (h >>> 16);
	}
	
}
