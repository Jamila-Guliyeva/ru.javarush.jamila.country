package ru.javarush.country;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StatefulRedisConnectionImpl;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisScriptingCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import ru.javarush.country.dao.CityDao;
import ru.javarush.country.dao.CountryDao;
import ru.javarush.country.entity.City;
import ru.javarush.country.entity.Country;
import ru.javarush.country.entity.CountryLanguage;
import ru.javarush.country.redis.CityCountry;
import ru.javarush.country.redis.Language;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

public class Main {
    private final SessionFactory sessionFactory;
    private final RedisClient redisClient;

    private final ObjectMapper objectMapper;

    private final CityDao cityDao;
    private final CountryDao countryDao;

    public Main() {
        sessionFactory = prepareRelationalDb();
        redisClient = prepareRedisClient();
        objectMapper = new ObjectMapper();
        cityDao = new CityDao(sessionFactory);
        countryDao = new CountryDao(sessionFactory);
    }

    private RedisClient prepareRedisClient() {
        RedisClient redisClient = RedisClient.create(RedisURI.create("localhost", 6379));
        try(StatefulRedisConnection<String, String> connection = redisClient.connect()){
            System.out.println("\n Connected to Redis \n");
        }
        return redisClient;
    }

    private SessionFactory prepareRelationalDb(){
        final SessionFactory sessionFactory;
        Properties properties = new Properties();
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.DRIVER, "com.p6spy.engine.spy.P6SpyDriver");
        properties.put(Environment.URL, "jdbc:p6spy:mysql://localhost:3306/world");
        properties.put(Environment.USER, "root");
        properties.put(Environment.PASS, "rootpassword");
        properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
        properties.put(Environment.HBM2DDL_AUTO, "validate");
        properties.put(Environment.STATEMENT_BATCH_SIZE, "100");


        sessionFactory = new Configuration()
                .addAnnotatedClass(City.class)
                .addAnnotatedClass(Country.class)
                .addAnnotatedClass(CountryLanguage.class)
                .addProperties(properties)
                .buildSessionFactory();

        return sessionFactory;
    }

    private void shutdown() {
        if (nonNull(sessionFactory)) {
            sessionFactory.close();
        }
        if (nonNull(redisClient)) {
            redisClient.shutdown();
        }
    }

    private List<City> fetchData(Main main){
        try(Session session = main.sessionFactory.getCurrentSession()){
            List<City> allCities = new ArrayList<>();
            session.beginTransaction();
            List<Country> countries = main.countryDao.getAll();
            int totalCount = main.cityDao.getTotalCount();
            int step = 500;

            for (int i = 0; i < totalCount; i+= step){
                allCities.addAll(main.cityDao.getItems(i, step));
            }
            session.getTransaction().commit();
            return allCities;
        }
    }

    public static void main(String[] args) {
        Main main = new Main();
        List<City> allCities = main.fetchData(main);
        List<CityCountry> preparedData = main.transformData(allCities);
        main.pushToRedis(preparedData);

        main.sessionFactory.getCurrentSession().close();

        List<Integer> ids = List.of(3, 2545, 123, 89,3458);

        long startRedis = System.currentTimeMillis();
        main.testRedisData(ids);
        long stopRedis = System.currentTimeMillis();

        long startMysql = System.currentTimeMillis();
        main.testMysqlData(ids);
        long stopMysql = System.currentTimeMillis();

        System.out.printf("%s:\t%d ms\n", "Redis", (stopRedis - startRedis));
        System.out.printf("%s:\t%d ms\n", "MySQL", (stopMysql - startMysql));
        main.shutdown();
    }

    private void pushToRedis(List<CityCountry> data) {
        try(StatefulRedisConnection<String, String> connection = redisClient.connect()){
            RedisStringCommands<String, String> sync = connection.sync();
            for(CityCountry country : data){
                try {
                    sync.set(String.valueOf(country.getId()), objectMapper.writeValueAsString(country));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private List<CityCountry> transformData(List<City> cities) {

        return cities.stream().map(city -> {
            CityCountry res = new CityCountry();
            res.setId(city.getId());
            res.setName(city.getName());
            res.setPopulation(city.getPopulation());
            res.setDistrict(city.getDistrict());

            Country country = city.getCountry();
            res.setAlternativeCode(country.getAlternativeCode());
            res.setContinent(country.getContinent());
            res.setCountryCode(country.getCode());
            res.setCountryName(country.getName());
            res.setPopulation(country.getPopulation());
            res.setCountryRegion(country.getRegion());
            res.setCountrySurfaceArea(country.getSurfaceArea());
            Set<CountryLanguage> countryLanguages = country.getLanguages();
            Set<Language> languages = countryLanguages.stream().map(c1 -> {
                Language language = new Language();
                language.setLanguage(c1.getLanguage());
                language.setOfficial(c1.getOfficial());
                language.setPercentage(c1.getPercentage());
                return language;
            }).collect(Collectors.toSet());
            res.setLanguages(languages);

            return res;
        }).collect(Collectors.toList());
    }

    private void testRedisData(List<Integer> ids){
        try(StatefulRedisConnection<String, String> connection = redisClient.connect()){
            RedisStringCommands<String, String> sync = connection.sync();
            for(Integer id : ids){
                String value = sync.get(String.valueOf(id));
                try {
                    objectMapper.readValue(value, CityCountry.class);
                } catch (JsonProcessingException e){
                    e.printStackTrace();
                }
            }
        }
    }

    private void testMysqlData(List<Integer> ids){
        try (Session session = sessionFactory.getCurrentSession()){
            session.beginTransaction();
            for(Integer id : ids){
                City city = cityDao.getById(id);
                Set<CountryLanguage> languages = city.getCountry().getLanguages();
            }
            session.getTransaction().commit();
        }
    }
}
