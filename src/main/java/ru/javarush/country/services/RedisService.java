package ru.javarush.country.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import ru.javarush.country.redis.CityCountry;

import java.util.List;

public class RedisService {

    private RedisClient redisClient;
    private ObjectMapper objectMapper;


    public RedisClient prepareRedisClient() {
        this.redisClient = RedisClient.create(RedisURI.create("localhost", 6379));
        try(StatefulRedisConnection<String, String> connection = redisClient.connect()){
            System.out.println("\n Connected to Redis \n");
        }
        return redisClient;
    }

    public RedisClient getRedisClient() {
        return redisClient;
    }

    public void setRedisClient(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    public void testRedisData(List<Integer> ids, DBService dbService){
        this.redisClient = dbService.getRedisClient();
        this.objectMapper = dbService.getObjectMapper();
        try(StatefulRedisConnection<String, String> connection = this.redisClient.connect()){
            RedisStringCommands<String, String> sync = connection.sync();
            for(Integer id : ids){
                String value = sync.get(String.valueOf(id));
                try {
                   this.objectMapper.readValue(value, CityCountry.class);
                } catch (JsonProcessingException e){
                    e.printStackTrace();
                }
            }
        }
    }
}
