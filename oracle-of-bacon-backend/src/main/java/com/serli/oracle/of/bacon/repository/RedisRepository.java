package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {
    private final Jedis jedis;
    private static final String SEARCHES_KEY = "searches";
    private static final String HOST_KEY = "localhost"

    public RedisRepository() {
        this.jedis = new Jedis(HOST_KEY);
    }

    public List<String> getLastTenSearches() {
        return jedis.lrange(KEY,0,-1);
    }

    /**
     * Ajoute l'élément recherché en début de cache avant de garder les 10 derniers 
     */
    public void addSearchToCache(String name) {
        jedis.lpush(KEY, name);
        jedis.ltrim(KEY, 0, 9);
    }
}
