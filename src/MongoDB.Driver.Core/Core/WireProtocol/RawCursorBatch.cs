/* Copyright 2013-present MongoDB Inc.
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

using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.WireProtocol
{
    /// <summary>
    /// Represents one result batch (returned from either a Query or a GetMore message)
    /// </summary>
    public struct RawCursorBatch
    {
        // fields
        private readonly long _cursorId;
        private readonly RawBsonDocument _document;
        private readonly BsonDocument _postBatchResumeToken;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="CursorBatch{TDocument}"/> struct.
        /// </summary>
        /// <param name="cursorId">The cursor identifier.</param>
        /// <param name="postBatchResumeToken">The post batch resume token.</param>
        /// <param name="document">The documents.</param>
        public RawCursorBatch(
            long cursorId,
            BsonDocument postBatchResumeToken,
            RawBsonDocument document)
        {
            _cursorId = cursorId;
            _postBatchResumeToken = postBatchResumeToken;
            _document = Ensure.IsNotNull(document, nameof(document));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CursorBatch{TDocument}"/> struct.
        /// </summary>
        /// <param name="cursorId">The cursor identifier.</param>
        /// <param name="document">The documents.</param>
        public RawCursorBatch(long cursorId, RawBsonDocument document)
            : this(cursorId, null, document)
        {
        }

        // properties
        /// <summary>
        /// Gets the cursor identifier.
        /// </summary>
        /// <value>
        /// The cursor identifier.
        /// </value>
        public long CursorId
        {
            get { return _cursorId; }
        }

        /// <summary>
        /// Gets the documents.
        /// </summary>
        /// <value>
        /// The documents.
        /// </value>
        public RawBsonDocument Document
        {
            get { return _document; }
        }

        /// <summary>
        /// Gets the post batch resume token.
        /// </summary>
        /// <value>
        /// The post batch resume token.
        /// </value>
        public BsonDocument PostBatchResumeToken
        {
            get { return _postBatchResumeToken; }
        }
    }
}
