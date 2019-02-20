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

    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient client = ElasticSearchRepository.createClient();

        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }
        
        CreateIndexRequest indexRequest = new CreateIndexRequest("actors");
        CreateIndexResponse createIndexResponse = client.indices().create(indexRequest);
        
        PutMappingRequest request = new PutMappingRequest("actors"); 
        request.type("actor");
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
                System.out.println("Inserted total of " + count.get() + " actors");
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
                        IndexRequest requestLine = new IndexRequest("actors", "actor");
                    	Map<String, Object> jsonMap = new HashMap<>();
                        String name = line.substring(1, line.length()-1);
                        jsonMap.put("name", name);
                        String[] splittedName = name.split("\\s+");
                        jsonMap.put("suggest", splittedName);
                        requestLine.source(jsonMap);
                        //TODO ElasticSearch insert
                        bulkProcessor.add(requestLine);
                    });
            bulkProcessor.close();            
        }
        
        

        System.out.println("Inserted total of " + count.get() + " actors");

        client.close();
    }
}
