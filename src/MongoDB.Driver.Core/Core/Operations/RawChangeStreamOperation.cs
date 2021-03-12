/* Copyright 2017-present MongoDB Inc.
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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;

namespace MongoDB.Driver.Core.Operations
{
    /// <inheritdoc />
    public class RawChangeStreamOperation<TResult> : ChangeStreamOperation<TResult>, IChangeStreamOperation<TResult>
    {
        BsonDocument IChangeStreamOperation<TResult>.ResumeAfter { get => ResumeAfter; set => ResumeAfter = value; }
        BsonDocument IChangeStreamOperation<TResult>.StartAfter { get => StartAfter; set => StartAfter = value; }
        BsonTimestamp IChangeStreamOperation<TResult>.StartAtOperationTime { get => StartAtOperationTime; set => StartAtOperationTime = value; }

        /// <inheritdoc />
        public RawChangeStreamOperation(IEnumerable<BsonDocument> pipeline, IBsonSerializer<TResult> resultSerializer, MessageEncoderSettings messageEncoderSettings)
            : base(pipeline, resultSerializer, messageEncoderSettings) { }

        /// <inheritdoc />
        public RawChangeStreamOperation(DatabaseNamespace databaseNamespace, IEnumerable<BsonDocument> pipeline, IBsonSerializer<TResult> resultSerializer, MessageEncoderSettings messageEncoderSettings)
            : base(databaseNamespace, pipeline, resultSerializer, messageEncoderSettings) { }

        /// <inheritdoc />
        public RawChangeStreamOperation(
            CollectionNamespace collectionNamespace, IEnumerable<BsonDocument> pipeline, IBsonSerializer<TResult> resultSerializer, MessageEncoderSettings messageEncoderSettings)
            : base(collectionNamespace, pipeline, resultSerializer, messageEncoderSettings) { }

        private BsonDocument CreateChangeStreamStage()
        {
            var changeStreamOptions = new BsonDocument
            {
                { "fullDocument", () => ToString(FullDocument), FullDocument != ChangeStreamFullDocumentOption.Default },
                { "allChangesForCluster", true, CollectionNamespace == null && DatabaseNamespace == null },
                { "startAfter", StartAfter, StartAfter != null},
                { "startAtOperationTime", StartAtOperationTime, StartAtOperationTime != null },
                { "resumeAfter", ResumeAfter, ResumeAfter != null }
            };
            return new BsonDocument("$changeStream", changeStreamOptions);
        }

        private string ToString(ChangeStreamFullDocumentOption fullDocument)
        {
            switch (fullDocument)
            {
                case ChangeStreamFullDocumentOption.Default: return "default";
                case ChangeStreamFullDocumentOption.UpdateLookup: return "updateLookup";
                default: throw new ArgumentException($"Invalid FullDocument option: {fullDocument}.", nameof(fullDocument));
            }
        }

        private List<BsonDocument> CreateCombinedPipeline(BsonDocument changeStreamStage)
        {
            var combinedPipeline = new List<BsonDocument>();
            combinedPipeline.Add(changeStreamStage);
            combinedPipeline.AddRange(Pipeline);
            return combinedPipeline;
        }

        private RawAggregateOperation CreateAggregateOperation()
        {
            var changeStreamStage = CreateChangeStreamStage();
            var combinedPipeline = CreateCombinedPipeline(changeStreamStage);

            RawAggregateOperation operation;
            if (CollectionNamespace != null)
            {
                operation = new RawAggregateOperation(CollectionNamespace, combinedPipeline, RawBsonDocumentSerializer.Instance, MessageEncoderSettings)
                {
                    RetryRequested = RetryRequested // might be overridden by retryable read context
                };
            }
            else
            {
                var databaseNamespace = DatabaseNamespace ?? DatabaseNamespace.Admin;
                operation = new RawAggregateOperation(databaseNamespace, combinedPipeline, RawBsonDocumentSerializer.Instance, MessageEncoderSettings)
                {
                    RetryRequested = RetryRequested // might be overridden by retryable read context
                };
            }

            operation.BatchSize = BatchSize;
            operation.Collation = Collation;
            operation.MaxAwaitTime = MaxAwaitTime;
            operation.ReadConcern = ReadConcern;

            return operation;
        }

        private IAsyncCursor<RawBsonDocument> ExecuteAggregateOperation(RetryableReadContext context, CancellationToken cancellationToken)
        {
            var aggregateOperation = CreateAggregateOperation();
            return aggregateOperation.Execute(context, cancellationToken);
        }

        private Task<IAsyncCursor<RawBsonDocument>> ExecuteAggregateOperationAsync(RetryableReadContext context, CancellationToken cancellationToken)
        {
            var aggregateOperation = CreateAggregateOperation();
            return aggregateOperation.ExecuteAsync(context, cancellationToken);
        }

        private BsonDocument GetInitialPostBatchResumeTokenIfRequired(ICursorBatchInfo cursorBatchInfo)
        {
            // If the initial aggregate returns an empty batch, but includes a `postBatchResumeToken`, then we should return that token.
            return cursorBatchInfo.WasFirstBatchEmpty ? cursorBatchInfo.PostBatchResumeToken : null;
        }

        private BsonTimestamp GetInitialOperationTimeIfRequired(RetryableReadContext context, ICursorBatchInfo cursorBatchInfo)
        {
            if (StartAtOperationTime == null && ResumeAfter == null && StartAfter == null)
            {
                var maxWireVersion = context.Channel.ConnectionDescription.IsMasterResult.MaxWireVersion;
                if (maxWireVersion >= 7)
                {
                    if (cursorBatchInfo.PostBatchResumeToken == null && cursorBatchInfo.WasFirstBatchEmpty)
                    {
                        return context.Binding.Session.OperationTime;
                    }
                }
            }

            return null;
        }

        IChangeStreamCursor<TResult> IReadOperation<IChangeStreamCursor<TResult>>.Execute(IReadBinding binding, CancellationToken cancellationToken)
        {
            Ensure.IsNotNull(binding, nameof(binding));
            var bindingHandle = binding as IReadBindingHandle;
            if (bindingHandle == null)
            {
                throw new ArgumentException("The binding value passed to ChangeStreamOperation.Execute must implement IReadBindingHandle.", nameof(binding));
            }

            IAsyncCursor<RawBsonDocument> cursor;
            ICursorBatchInfo cursorBatchInfo;
            BsonTimestamp initialOperationTime;
            using (var context = RetryableReadContext.Create(binding, RetryRequested, cancellationToken))
            {
                cursor = ExecuteAggregateOperation(context, cancellationToken);
                cursorBatchInfo = (ICursorBatchInfo)cursor;
                initialOperationTime = GetInitialOperationTimeIfRequired(context, cursorBatchInfo);

                var postBatchResumeToken = GetInitialPostBatchResumeTokenIfRequired(cursorBatchInfo);

                return new RawChangeStreamCursor<TResult>(
                    cursor,
                    ResultSerializer,
                    bindingHandle.Fork(),
                    this,
                    postBatchResumeToken,
                    initialOperationTime,
                    StartAfter,
                    ResumeAfter,
                    StartAtOperationTime,
                    context.Channel.ConnectionDescription.ServerVersion);
            }
        }

        async Task<IChangeStreamCursor<TResult>> IReadOperation<IChangeStreamCursor<TResult>>.ExecuteAsync(IReadBinding binding, CancellationToken cancellationToken)
        {
            Ensure.IsNotNull(binding, nameof(binding));
            var bindingHandle = binding as IReadBindingHandle;
            if (bindingHandle == null)
            {
                throw new ArgumentException("The binding value passed to ChangeStreamOperation.ExecuteAsync must implement IReadBindingHandle.", nameof(binding));
            }

            IAsyncCursor<RawBsonDocument> cursor;
            ICursorBatchInfo cursorBatchInfo;
            BsonTimestamp initialOperationTime;
            using (var context = await RetryableReadContext.CreateAsync(binding, RetryRequested, cancellationToken).ConfigureAwait(false))
            {
                cursor = await ExecuteAggregateOperationAsync(context, cancellationToken).ConfigureAwait(false);
                cursorBatchInfo = (ICursorBatchInfo)cursor;
                initialOperationTime = GetInitialOperationTimeIfRequired(context, cursorBatchInfo);

                var postBatchResumeToken = GetInitialPostBatchResumeTokenIfRequired(cursorBatchInfo);

                return new RawChangeStreamCursor<TResult>(
                    cursor,
                    ResultSerializer,
                    bindingHandle.Fork(),
                    this,
                    postBatchResumeToken,
                    initialOperationTime,
                    StartAfter,
                    ResumeAfter,
                    StartAtOperationTime,
                    context.Channel.ConnectionDescription.ServerVersion);
            }
        }

        IAsyncCursor<RawBsonDocument> IChangeStreamOperation<TResult>.Resume(IReadBinding binding, CancellationToken cancellationToken)
        {
            using (var context = RetryableReadContext.Create(binding, retryRequested: false, cancellationToken))
            {
                return ExecuteAggregateOperation(context, cancellationToken);
            }
        }

        async Task<IAsyncCursor<RawBsonDocument>> IChangeStreamOperation<TResult>.ResumeAsync(IReadBinding binding, CancellationToken cancellationToken)
        {
            using (var context = await RetryableReadContext.CreateAsync(binding, retryRequested: false, cancellationToken).ConfigureAwait(false))
            {
                return await ExecuteAggregateOperationAsync(context, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
