﻿/* Copyright 2019-present MongoDB Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System.Buffers;
using System.IO;
using System.Threading;
using MongoDB.Driver.Core.Compression.Snappy;
using MongoDB.Driver.Core.Misc;
#if NET452
using SnappyCodec = Snappy.SnappyCodec;
#endif

namespace MongoDB.Driver.Core.Compression
{
    internal class SnappyCompressor : ICompressor
    {
        public CompressorType Type => CompressorType.Snappy;

        /// <summary>
        /// Compresses the remainder of <paramref name="input"/>, writing the compressed data to
        /// <paramref name="output"/>.
        /// </summary>
        /// <param name="input"> The input stream.</param>
        /// <param name="output">The output stream.</param>
        public void Compress(Stream input, Stream output)
        {
#if NET452
            var uncompressedSize = (int)(input.Length - input.Position);
            //var uncompressedBytes = new byte[uncompressedSize]; // does not include uncompressed message headers
            var uncompressedBytes = ArrayPool<byte>.Shared.Rent(uncompressedSize); // does not include uncompressed message headers
            input.ReadBytes(uncompressedBytes, offset: 0, count: uncompressedSize, CancellationToken.None);
            var maxCompressedSize = SnappyCodec.GetMaxCompressedLength(uncompressedSize);
            //var compressedBytes = new byte[maxCompressedSize];
            var compressedBytes = ArrayPool<byte>.Shared.Rent(maxCompressedSize);
            //var compressedSize = SnappyCodec.Compress(
            //    input: uncompressedBytes,
            //    inputOffset: 0,
            //    inputLength: uncompressedSize,
            //    output: compressedBytes,
            //    outputOffset: 0,
            //    outputLength: compressedBytes.Length); // output.Length - outputOffset
            var compressedSize = SnappyCodec.Compress(uncompressedBytes, 0, uncompressedSize, compressedBytes, 0);
            output.Write(compressedBytes, 0, compressedSize);
            ArrayPool<byte>.Shared.Return(uncompressedBytes);
            ArrayPool<byte>.Shared.Return(compressedBytes);
#else
            using (var compressStream = IronSnappy.Snappy.OpenWriter(output))
            {
                input.EfficientCopyTo(compressStream);
                compressStream.Flush();
            }
#endif
        }

        /// <summary>
        /// Decompresses the remainder of  <paramref name="input"/>, writing the uncompressed data to <paramref name="output"/>.
        /// </summary>
        /// <param name="input">The input stream.</param>
        /// <param name="output">The output stream.</param>
        public void Decompress(Stream input, Stream output)
        {
#if NET452
            var compressedSize = (int)(input.Length - input.Position);
            //var compressedBytes = new byte[compressedSize];
            var compressedBytes = ArrayPool<byte>.Shared.Rent(compressedSize);
            input.ReadBytes(compressedBytes, offset: 0, count: compressedSize, CancellationToken.None);
            //var decompressedBytes = SnappyCodec.Uncompress(compressedBytes);
            var maxdeCompressedSize = SnappyCodec.GetUncompressedLength(compressedBytes);
            var decompressedBytes = ArrayPool<byte>.Shared.Rent(maxdeCompressedSize);
            var decompressedSize = SnappyCodec.Uncompress(compressedBytes, 0, compressedSize, decompressedBytes, 0);
            //output.Write(decompressedBytes, offset: 0, count: decompressedBytes.Length);
            output.Write(decompressedBytes, 0, decompressedSize);
            ArrayPool<byte>.Shared.Return(compressedBytes);
            ArrayPool<byte>.Shared.Return(decompressedBytes);
#else
            using (var decompressStream = IronSnappy.Snappy.OpenReader(input))
            {
                decompressStream.EfficientCopyTo(output);
                decompressStream.Flush();
            }
#endif
        }
    }
}
