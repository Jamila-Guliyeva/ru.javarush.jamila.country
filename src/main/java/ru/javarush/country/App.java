package ru.javarush.country;

import org.hibernate.SessionFactory;
import ru.javarush.country.entity.City;
import ru.javarush.country.redis.CityCountry;
import ru.javarush.country.service.DBMapper;
import ru.javarush.country.service.RedisMapper;
import ru.javarush.country.service.RelationalDBMapper;

import java.util.List;


public class App {

    public static void main(String[] args) {
        DBMapper dbMapper = new DBMapper();
        RelationalDBMapper relationalDBMapper = new RelationalDBMapper();
        SessionFactory sessionFactory = dbMapper.getSessionFactory();
        RedisMapper redisMapper = new RedisMapper();


        List<City> allCities = dbMapper.getRelationalDBService().fetchData(dbMapper);
        List<CityCountry> preparedData = relationalDBMapper.transformData(allCities);
        relationalDBMapper.pushToRedis(preparedData, dbMapper);

        sessionFactory.getCurrentSession().close();

        List<Integer> ids = List.of(3, 2545, 123, 4, 189, 89, 3458, 1189, 10, 102);

        long startRedis = System.currentTimeMillis();
        redisMapper.testRedisData(ids, dbMapper);
        long stopRedis = System.currentTimeMillis();

        long startMysql = System.currentTimeMillis();
        relationalDBMapper.testMysqlData(ids, dbMapper);
        long stopMysql = System.currentTimeMillis();

        System.out.printf("%s:\t%d ms\n", "Redis", (stopRedis - startRedis));
        System.out.printf("%s:\t%d ms\n", "MySQL", (stopMysql - startMysql));
        dbMapper.shutdown();
    }
}
