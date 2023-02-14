package ru.javarush.country.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import org.hibernate.SessionFactory;
import ru.javarush.country.dao.CityDao;
import ru.javarush.country.dao.CountryDao;

import static java.util.Objects.nonNull;



public class DBService {

    private final SessionFactory sessionFactory;
    private final RedisClient redisClient;
    private final ObjectMapper objectMapper;
    private final CityDao cityDao;
    private final CountryDao countryDao;
    private final RedisService redisService;
    private final RelationalDBService relationalDBService;

    public DBService() {
        relationalDBService = new RelationalDBService();
        sessionFactory = relationalDBService.prepareRelationalDb();
        redisService = new RedisService();
        redisClient = redisService.prepareRedisClient();
        objectMapper = new ObjectMapper();
        cityDao = new CityDao(sessionFactory);
        countryDao = new CountryDao(sessionFactory);
    }

    public void shutdown() {
        if (nonNull(sessionFactory)) {
            sessionFactory.close();
        }
        if (nonNull(redisClient)) {
            redisClient.shutdown();
        }
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public RedisClient getRedisClient() {
        return redisClient;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public CityDao getCityDao() {
        return cityDao;
    }

    public CountryDao getCountryDao() {
        return countryDao;
    }

    public RedisService getRedisService() {
        return redisService;
    }

    public RelationalDBService getRelationalDBService() {
        return relationalDBService;
    }
}
