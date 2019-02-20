package com.serli.oracle.of.bacon.repository;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

import org.bson.Document;

import java.util.Optional;

public class MongoDbRepository {
    private final MongoCollection<Document> actorCollection;
    private static final String HOST_KEY = "localhost";
    private static final String DB_KEY = "workshop";
    private static final String COLLECTION_KEY = "actors";
    private static final String NAME_KEY = "name";

    public MongoDbRepository() {
        this.actorCollection= new MongoClient(HOST_KEY, 27017).getDatabase(DB_KEY).getCollection(COLLECTION_KEY);
    }

    public Optional<Document> getActorByName(String name) {
        return Optional.ofNullable(this.actorCollection.find(Filters.eq(NAME_KEY, name)).first());
    }
}
