package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);
    private static final String ACTORS_KEY = "actors";
    private static final String ACTOR_KEY = "actor";
    private static final String NAME_KEY = "name";
    private static final String SUGGEST_KEY = "suggest";
    private static final String SPACE_REGEXP = "\\s+";

    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient client = ElasticSearchRepository.createClient();

        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }
        
        CreateIndexRequest indexRequest = new CreateIndexRequest(ACTORS_KEY);
        CreateIndexResponse createIndexResponse = client.indices().create(indexRequest);
        
        PutMappingRequest request = new PutMappingRequest(ACTORS_KEY); 
        request.type(ACTOR_KEY);
        String jsonBody = "{ "
        		+ "\"properties\": "
        		+ "{ \"suggest\":    "
        		+ "	{ \"type\": \"completion\"  }"
        		+ ","
        		+ "\"name\" : "
        		+ "	{\"type\": \"text\"}"
        		+ "}}";
        request.source(jsonBody, XContentType.JSON);
        AcknowledgedResponse putMappingResponse = client.indices().putMapping(request);
        
        BulkProcessor.Listener listener = new BulkProcessor.Listener() { 
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                    BulkResponse response) {
                count.incrementAndGet();
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                    Throwable failure) {
                System.err.println("fail");
            }
        };
        
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (bulkRequest, bulkListener) ->
                    client.bulkAsync(bulkRequest, bulkListener);
        
        BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, listener);
        builder.setBulkActions(50000); 
        builder.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB));
        builder.setFlushInterval(TimeValue.timeValueSeconds(10));
        builder.setConcurrentRequests(0);
        
        BulkProcessor bulkProcessor = builder.build();

        String inputFilePath = args[0];
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader
                    .lines()
                    .forEach(line -> {
                    	if (count.get() < 1) {
                    		count.incrementAndGet();
                    		return;
                    	}
                        IndexRequest requestLine = new IndexRequest(ACTORS_KEY, ACTOR_KEY);
                    	Map<String, Object> jsonMap = new HashMap<>();
                        String name = line.substring(1, line.length()-1);
                        jsonMap.put(NAME_KEY, name);
                        String[] splittedName = name.split(SPACE_REGEXP);
                        jsonMap.put(SUGGEST_KEY, splittedName);
                        requestLine.source(jsonMap);
                        bulkProcessor.add(requestLine);
                    });
            bulkProcessor.close();            
        }
        System.out.println("Inserted total of " + count.get() + " actors bulk");
        client.close();
    }
}
