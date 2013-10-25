#/**
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
# -------------------------------------------------------------------------------------
# multi val params are iterated by NewRound's, added to reports, start with column name.
#



log.step=5000

docs.file=/home/mark/enwiki/enwiki-latest-pages-articles.txt
content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource

solr.server=solrserver:org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer:org.apache.solr.client.solrj.impl.HttpSolrServer
solr.streaming.server.queue.size=10
solr.streaming.server.threadcount=4:4

solr.internal.server.xmx=1000M

#solr.configs.home=conf/solr/configs/filters
#solr.schema=schema:schema_no_filter.xml

solr.field.mappings=doctitle>title,docid>id,docname>body_txt,docdate>body_txt,docdatenum>body_txt,doctimesecnum>body_t,body>body_txt

query.maker=org.apache.lucene.benchmark.byTask.feeds.ReutersQueryMaker

# task at this depth or less would print when they start
task.max.depth.log=2

log.queries=false
# -------------------------------------------------------------------------------------

{ "Rounds"

	solr.StopSolrServer
	
    ResetSystemErase
    
    solr.StartSolrServer(log)
    
    solr.SolrClearIndex
    
    {"AddDocs"
      [ 
          { "MAddDocs" solr.SolrAddDoc > : 8000
 
      ] : 1
      { "BlockTillFinish" solr.BlockSolrServerTillFinish >
    }
    
	solr.SolrCommit
	
	solr.StopSolrServer
		
    NewRound
	
} : 2


RepSumByPrefRound AddDocs
