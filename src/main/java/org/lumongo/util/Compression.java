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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class Compression {
	private Compression() {

	}

	public enum CompressionLevel {
		BEST(Deflater.BEST_COMPRESSION),
		NORMAL(Deflater.DEFAULT_COMPRESSION),
		FASTEST(Deflater.BEST_SPEED);

		private int level;

		CompressionLevel(int level) {
			this.level = level;
		}

		public int getLevel() {
			return level;
		}
	}

	public static byte[] compressZlib(byte[] bytes, CompressionLevel compressionLevel) {
		Deflater compressor = new Deflater();
		compressor.setLevel(compressionLevel.getLevel());
		compressor.setInput(bytes);
		compressor.finish();

		int bufferLength = Math.max(bytes.length / 10, 16);
		byte[] buf = new byte[bufferLength];
		ByteArrayOutputStream bos = new ByteArrayOutputStream(bufferLength);
		while (!compressor.finished()) {
			int count = compressor.deflate(buf);
			bos.write(buf, 0, count);
		}
		try {
			bos.close();
		}
		catch (Exception e) {

		}
		compressor.end();
		return bos.toByteArray();
	}

	public static byte[] uncompressZlib(byte[] bytes) throws IOException {
		Inflater inflater = new Inflater();
		inflater.setInput(bytes);
		ByteArrayOutputStream bos = new ByteArrayOutputStream(bytes.length);
		byte[] buf = new byte[1024];
		while (!inflater.finished()) {
			try {
				int count = inflater.inflate(buf);
				bos.write(buf, 0, count);
			}
			catch (DataFormatException e) {
			}
		}
		bos.close();
		inflater.end();
		return bos.toByteArray();
	}

}
