package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {
    private final Jedis jedis;
    private final String KEY = "searches";

    public RedisRepository() {
        this.jedis = new Jedis("localhost");
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
