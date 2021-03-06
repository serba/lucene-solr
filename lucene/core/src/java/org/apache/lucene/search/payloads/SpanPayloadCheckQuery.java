package org.apache.lucene.search.payloads;
/*
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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.FilterSpans.AcceptStatus;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanPositionCheckQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;


/**
 * Only return those matches that have a specific payload at
 * the given position.
 * <p>
 * Do not use this with a SpanQuery that contains a {@link org.apache.lucene.search.spans.SpanNearQuery}.
 * Instead, use {@link org.apache.lucene.search.payloads.SpanNearPayloadCheckQuery} since it properly handles the fact that payloads
 * aren't ordered by {@link org.apache.lucene.search.spans.SpanNearQuery}.
 */
public class SpanPayloadCheckQuery extends SpanPositionCheckQuery {

  protected final Collection<byte[]> payloadToMatch;

  /**
   * @param match The underlying {@link org.apache.lucene.search.spans.SpanQuery} to check
   * @param payloadToMatch The {@link java.util.Collection} of payloads to match
   */
  public SpanPayloadCheckQuery(SpanQuery match, Collection<byte[]> payloadToMatch) {
    super(match);
    if (match instanceof SpanNearQuery){
      throw new IllegalArgumentException("SpanNearQuery not allowed");
    }
    this.payloadToMatch = payloadToMatch;
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return super.createWeight(searcher, needsScores, PayloadSpanCollector.FACTORY);
  }

  @Override
  protected AcceptStatus acceptPosition(Spans spans, SpanCollector collector) throws IOException {

    PayloadSpanCollector payloadCollector = (PayloadSpanCollector) collector;

    payloadCollector.reset();
    spans.collect(payloadCollector);

    Collection<byte[]> candidate = payloadCollector.getPayloads();
    if (candidate.size() == payloadToMatch.size()){
      //TODO: check the byte arrays are the same
      Iterator<byte[]> toMatchIter = payloadToMatch.iterator();
      //check each of the byte arrays, in order
      //hmm, can't rely on order here
      for (byte[] candBytes : candidate) {
        //if one is a mismatch, then return false
        if (Arrays.equals(candBytes, toMatchIter.next()) == false){
          return AcceptStatus.NO;
        }
      }
      //we've verified all the bytes
      return AcceptStatus.YES;
    } else {
      return AcceptStatus.NO;
    }

  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanPayCheck(");
    buffer.append(match.toString(field));
    buffer.append(", payloadRef: ");
    for (byte[] bytes : payloadToMatch) {
      ToStringUtils.byteArray(buffer, bytes);
      buffer.append(';');
    }
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public SpanPayloadCheckQuery clone() {
    SpanPayloadCheckQuery result = new SpanPayloadCheckQuery((SpanQuery) match.clone(), payloadToMatch);
    result.setBoost(getBoost());
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (! super.equals(o)) {
      return false;
    }
    SpanPayloadCheckQuery other = (SpanPayloadCheckQuery)o;
    return this.payloadToMatch.equals(other.payloadToMatch);
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = (h * 63) ^ payloadToMatch.hashCode();
    return h;
  }
}