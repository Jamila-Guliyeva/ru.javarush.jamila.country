package ru.javarush.country.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import ru.javarush.country.dao.CityDao;
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

public class RelationalDBService {

    private RedisClient redisClient;
    private ObjectMapper objectMapper;
    private DBService dbService;
    private SessionFactory sessionFactory;

    public SessionFactory prepareRelationalDb(){
        Properties properties = new Properties();
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.DRIVER, "com.p6spy.engine.spy.P6SpyDriver");
        properties.put(Environment.URL, "jdbc:p6spy:mysql://localhost:3306/world");
        properties.put(Environment.USER, "root");
        properties.put(Environment.PASS, "rootpassword");
        properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
        properties.put(Environment.HBM2DDL_AUTO, "validate");
        properties.put(Environment.STATEMENT_BATCH_SIZE, "100");


        this.sessionFactory = new Configuration()
                .addAnnotatedClass(City.class)
                .addAnnotatedClass(Country.class)
                .addAnnotatedClass(CountryLanguage.class)
                .addProperties(properties)
                .buildSessionFactory();

        return this.sessionFactory;
    }

    public List<City> fetchData(DBService dbService){
        this.sessionFactory = dbService.getSessionFactory();
        CityDao cityDao = dbService.getCityDao();

        try(Session session = this.sessionFactory.getCurrentSession()){
            List<City> allCities = new ArrayList<>();
            session.beginTransaction();
            int totalCount = cityDao.getTotalCount();
            int step = 500;

            for (int i = 0; i < totalCount; i+= step){
                allCities.addAll(cityDao.getItems(i, step));
            }
            session.getTransaction().commit();
            return allCities;
        }
    }

    public void testMysqlData(List<Integer> ids, DBService dbService){
        this.sessionFactory = dbService.getSessionFactory();
        CityDao cityDao = dbService.getCityDao();

        try (Session session = this.sessionFactory.getCurrentSession()){
            session.beginTransaction();
            for(Integer id : ids){
                City city = cityDao.getById(id);
                Set<CountryLanguage> languages = city.getCountry().getLanguages();
            }
            session.getTransaction().commit();
        }
    }

    public void pushToRedis(List<CityCountry> data, DBService dbService) {
        this.redisClient = dbService.getRedisClient();
        this.objectMapper = dbService.getObjectMapper();

        try(StatefulRedisConnection<String, String> connection = this.redisClient.connect()){
            RedisStringCommands<String, String> sync = connection.sync();
            for(CityCountry country : data){
                try {
                    sync.set(String.valueOf(country.getId()), this.objectMapper.writeValueAsString(country));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public List<CityCountry> transformData(List<City> cities) {

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

}
