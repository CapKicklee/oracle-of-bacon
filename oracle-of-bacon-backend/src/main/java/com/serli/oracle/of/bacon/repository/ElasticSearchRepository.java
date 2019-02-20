package com.serli.oracle.of.bacon.repository;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticSearchRepository {

    private final RestHighLevelClient client;
    private static final String ACTORS_KEY = "actors";
    private static final String SUGGEST_KEY = "suggest";
    private static final String SUGGEST_ACTOR_KEY = "suggest_actor";
    private static final String NAME_KEY = "name";

    public ElasticSearchRepository() {
        client = createClient();

    }

    public static RestHighLevelClient createClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        );
    }

    public List<String> getActorsSuggests(String searchQuery) throws IOException {
        List<String> suggestions = new ArrayList<String>();
        SearchRequest searchRequest = new SearchRequest(ACTORS_KEY);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SuggestionBuilder termSuggestionBuilder = SuggestBuilders.completionSuggestion(SUGGEST_KEY).prefix(searchQuery); 
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion(SUGGEST_ACTOR_KEY, termSuggestionBuilder); 
        searchSourceBuilder.suggest(suggestBuilder);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);
        Suggest suggest = searchResponse.getSuggest(); 
        CompletionSuggestion completionSuggestion = suggest.getSuggestion(SUGGEST_ACTOR_KEY); 
        for (CompletionSuggestion.Entry entry : completionSuggestion.getEntries()) { 
            for (CompletionSuggestion.Entry.Option option : entry) { 
                String suggestText = option.getHit().getSourceAsMap().get(NAME_KEY).toString();
                suggestions.add(suggestText);
            }
        }

        return suggestions;
    }
}
