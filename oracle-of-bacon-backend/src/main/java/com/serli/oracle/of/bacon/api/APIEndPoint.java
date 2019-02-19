package com.serli.oracle.of.bacon.api;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import com.serli.oracle.of.bacon.repository.MongoDbRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository;
import com.serli.oracle.of.bacon.repository.RedisRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository.GraphItem;

import org.neo4j.driver.v1.Value;

import net.codestory.http.annotations.Get;
import net.codestory.http.convert.TypeConvert;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class APIEndPoint {
    private final Neo4JRepository neo4JRepository;
    private final ElasticSearchRepository elasticSearchRepository;
    private final RedisRepository redisRepository;
    private final MongoDbRepository mongoDbRepository;

    public APIEndPoint() {
        neo4JRepository = new Neo4JRepository();
        elasticSearchRepository = new ElasticSearchRepository();
        redisRepository = new RedisRepository();
        mongoDbRepository = new MongoDbRepository();
    }

    @Get("bacon-to?actor=:actorName")
    public String getConnectionsToKevinBacon(String actorName) {
        String parsedActorName = "";
        try {
            parsedActorName = java.net.URLDecoder.decode(actorName, "UTF-8");
            redisRepository.addSearchToCache(parsedActorName);
        } catch (UnsupportedEncodingException ignored) {
        }

        List<GraphItem> listeConnections = neo4JRepository.getConnectionsToKevinBacon(parsedActorName);

        List<Map<String, GraphItem>> data = new ArrayList<Map<String, GraphItem>>();
        for (GraphItem values : listeConnections) {
            Map<String, GraphItem> map = new HashMap<String, GraphItem>();
            map.put("data", values);
            data.add(map);
        }

        String json = TypeConvert.toJson(data);
        return json;
    }

    @Get("suggest?q=:searchQuery")
    public List<String> getActorSuggestion(String searchQuery) throws IOException {
        return Arrays.asList("Niro, Chel",
                "Senanayake, Niro",
                "Niro, Juan Carlos",
                "de la Rua, Niro",
                "Niro, Simão");
    }

    @Get("last-searches")
    public List<String> last10Searches() {
        return redisRepository.getLastTenSearches();
    }

    @Get("actor?name=:actorName")
    public String getActorByName(String actorName) {
        return this.mongoDbRepository.getActorByName(actorName).get().toJson();
    }
}
