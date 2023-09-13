package org.lumongo.storage.lucene;

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

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;

public class DistributedIndexInput extends IndexInput {
	private final NosqlFile nosqlFile;

	protected long position;
	protected long sliceOffset;
	protected long length;

	public DistributedIndexInput(String name, NosqlFile nosqlFile) {
		this(name, nosqlFile, 0, nosqlFile.getFileLength());
	}

	public DistributedIndexInput(String name, NosqlFile nosqlFile, long sliceOffset, long length) {
		super(DistributedIndexInput.class.getSimpleName() + "(" + name + ")");
		this.nosqlFile = nosqlFile;
		this.sliceOffset = sliceOffset;
		this.length = length;
	}

	@Override
	public void close() throws IOException {
		//NO-OP
	}

	@Override
	public long getFilePointer() {
		return position;
	}

	@Override
	public void seek(long pos) throws IOException {
		this.position = pos;

	}

	@Override
	public long length() {
		return length;
	}

	@Override
	public byte readByte() throws IOException {
		if (position >= length) {
			throw new EOFException("read past EOF: " + this);
		}

		byte result = nosqlFile.readByte(position + sliceOffset);
		position += 1;
		return result;
	}

	@Override
	public int readInt() throws IOException {
		int result = nosqlFile.readInt(position + sliceOffset);
		position += 4;
		return result;
	}

	@Override
	public long readLong() throws IOException {
		long result = nosqlFile.readLong(position + sliceOffset);
		position += 8;
		return result;
	}

	@Override
	public void readBytes(byte[] b, int offset, int length) throws IOException {

		if (position + length > this.length) {
			throw new EOFException("read past EOF: " + this);
		}

		nosqlFile.readBytes(position + sliceOffset, b, offset, length);
		position += length;
	}

	@Override
	public IndexInput slice(String sliceDescription, final long sliceOffset, final long length) throws IOException {
		if (sliceOffset < 0 || length < 0 || sliceOffset + length > this.length) {
			throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: "  + this);
		}
		final DistributedIndexInput dii = new DistributedIndexInput(getFullSliceDescription(sliceDescription), nosqlFile, this.sliceOffset + sliceOffset, length);
		return dii;
	}

	@Override
	public IndexInput clone() {
		IndexInput ii = new DistributedIndexInput(getFullSliceDescription(null), nosqlFile, sliceOffset, length);
		try {
			ii.seek(getFilePointer());
		}
		catch (IOException ioe) {
			throw new AssertionError(ioe);
		}

		return ii;
	}
}
