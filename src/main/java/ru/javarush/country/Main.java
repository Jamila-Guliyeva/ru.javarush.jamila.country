package ru.javarush.country;

import org.hibernate.SessionFactory;
import ru.javarush.country.entity.City;
import ru.javarush.country.redis.CityCountry;
import ru.javarush.country.services.DBService;
import ru.javarush.country.services.RedisService;
import ru.javarush.country.services.RelationalDBService;

import java.util.List;


public class Main {

    public static void main(String[] args) {
        DBService dbService = new DBService();
        RelationalDBService relationalDBService = new RelationalDBService();
        SessionFactory sessionFactory = dbService.getSessionFactory();
        RedisService redisService = new RedisService();


        List<City> allCities = dbService.getRelationalDBService().fetchData(dbService);
        List<CityCountry> preparedData = relationalDBService.transformData(allCities);
        relationalDBService.pushToRedis(preparedData, dbService);

        sessionFactory.getCurrentSession().close();

        List<Integer> ids = List.of(3, 2545, 123, 4, 189, 89, 3458, 1189, 10, 102);

        long startRedis = System.currentTimeMillis();
        redisService.testRedisData(ids, dbService);
        long stopRedis = System.currentTimeMillis();

        long startMysql = System.currentTimeMillis();
        relationalDBService.testMysqlData(ids, dbService);
        long stopMysql = System.currentTimeMillis();

        System.out.printf("%s:\t%d ms\n", "Redis", (stopRedis - startRedis));
        System.out.printf("%s:\t%d ms\n", "MySQL", (stopMysql - startMysql));
        dbService.shutdown();
    }
}
