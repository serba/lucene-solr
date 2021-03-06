package org.apache.solr.handler;

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

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Schedule the execution of the {@link org.apache.solr.handler.CdcrReplicator} threads at
 * regular time interval. It relies on a queue of {@link org.apache.solr.handler.CdcrReplicatorState} in
 * order to avoid that one {@link org.apache.solr.handler.CdcrReplicatorState} is used by two threads at the same
 * time.
 */
class CdcrReplicatorScheduler {

  private boolean isStarted = false;

  private ScheduledExecutorService scheduler;
  private ExecutorService replicatorsPool;

  private final CdcrReplicatorManager replicatorManager;
  private final ConcurrentLinkedQueue<CdcrReplicatorState> statesQueue;

  private int poolSize = DEFAULT_POOL_SIZE;
  private int timeSchedule = DEFAULT_TIME_SCHEDULE;
  private int batchSize = DEFAULT_BATCH_SIZE;

  private static final int DEFAULT_POOL_SIZE = 2;
  private static final int DEFAULT_TIME_SCHEDULE = 10;
  private static final int DEFAULT_BATCH_SIZE = 128;

  protected static Logger log = LoggerFactory.getLogger(CdcrReplicatorScheduler.class);

  CdcrReplicatorScheduler(final CdcrReplicatorManager replicatorStatesManager, final SolrParams replicatorConfiguration) {
    this.replicatorManager = replicatorStatesManager;
    this.statesQueue = new ConcurrentLinkedQueue<>(replicatorManager.getReplicatorStates());
    if (replicatorConfiguration != null) {
      poolSize = replicatorConfiguration.getInt(CdcrParams.THREAD_POOL_SIZE_PARAM, DEFAULT_POOL_SIZE);
      timeSchedule = replicatorConfiguration.getInt(CdcrParams.SCHEDULE_PARAM, DEFAULT_TIME_SCHEDULE);
      batchSize = replicatorConfiguration.getInt(CdcrParams.BATCH_SIZE_PARAM, DEFAULT_BATCH_SIZE);
    }
  }

  void start() {
    if (!isStarted) {
      scheduler = Executors.newSingleThreadScheduledExecutor(new DefaultSolrThreadFactory("cdcr-scheduler"));
      //replicatorsPool = Executors.newFixedThreadPool(poolSize, new DefaultSolrThreadFactory("cdcr-replicator"));
      replicatorsPool = ExecutorUtil.newMDCAwareFixedThreadPool(poolSize, new DefaultSolrThreadFactory("cdcr-replicator"));

      // the scheduler thread is executed every second and submits one replication task
      // per available state in the queue
      scheduler.scheduleWithFixedDelay(new Runnable() {

        @Override
        public void run() {
          int nCandidates = statesQueue.size();
          for (int i = 0; i < nCandidates; i++) {
            // a thread that pool one state from the queue, execute the replication task, and push back
            // the state in the queue when the task is completed
            replicatorsPool.execute(new Runnable() {

              @Override
              public void run() {
                CdcrReplicatorState state = statesQueue.poll();
                try {
                  new CdcrReplicator(state, batchSize).run();
                } finally {
                  statesQueue.offer(state);
                }
              }

            });

          }
        }

      }, 0, timeSchedule, TimeUnit.MILLISECONDS);
      isStarted = true;
    }
  }

  void shutdown() {
    if (isStarted) {
      replicatorsPool.shutdown();
      try {
        replicatorsPool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        log.warn("Thread interrupted while waiting for CDCR replicator threadpool close.");
        Thread.currentThread().interrupt();
      } finally {
        scheduler.shutdownNow();
        isStarted = false;
      }
    }
  }

}

