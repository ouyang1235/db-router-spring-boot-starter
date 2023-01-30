package cn.ouyang.middleware.db.router.config;

import cn.ouyang.middleware.db.router.DBRouterConfig;
import cn.ouyang.middleware.db.router.dynamic.DynamicDataSource;
import cn.ouyang.middleware.db.router.util.PropertyUtil;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class DataSourceAutoConfig implements EnvironmentAware {

    /**
     * 数据源配置组
     */
    private Map<String, Map<String,Object>> dataSourceMap = new HashMap<>();

    /**
     * 默认数据源配置
     */
    private Map<String,Object> defaultDataSourceConfig;

    /**
     * 分库数量
     */
    private int dbCount;

    /**
     * 分表数量
     */
    private int tbCount;

    /**
     * 路由字段
     */
    private String routerKey;


    @Override
    public void setEnvironment(Environment environment) {
        String prefix = "mini-db-router.jdbc.datasource.";
        //库表数量
        dbCount = Integer.valueOf(environment.getProperty(prefix + "dbCount"));
        tbCount = Integer.valueOf(environment.getProperty(prefix + "tbCount"));
        //分库分表字段
        routerKey = environment.getProperty(prefix + "routerKey");
        //分表分库数据源
        String dataSourcesStr = environment.getProperty(prefix + "list");
        assert dataSourcesStr != null;
        for (String dbInfo : dataSourcesStr.split(",")) {
            Map<String,Object> dataSourceProps = PropertyUtil.handle(environment, prefix + dbInfo, Map.class);
            dataSourceMap.put(dbInfo,dataSourceProps);
        }
        //默认数据源
        String defaultData = environment.getProperty(prefix + "default");
        defaultDataSourceConfig = PropertyUtil.handle(environment, prefix + defaultData, Map.class);
    }

    @Bean
    public DataSource dataSource(){
        //创建数据源
        Map<Object,Object> targetDataSource = new HashMap<>();
        for (String dbInfo : dataSourceMap.keySet()) {
            Map<String, Object> objMap = dataSourceMap.get(dbInfo);
            targetDataSource.put(dbInfo,new DriverManagerDataSource(objMap.get("url").toString(), objMap.get("username").toString(), objMap.get("password").toString()));
        }

        //设置数据源
        DynamicDataSource dynamicDataSource = new DynamicDataSource();
        dynamicDataSource.setTargetDataSources(targetDataSource);
        dynamicDataSource.setDefaultTargetDataSource(new DriverManagerDataSource(defaultDataSourceConfig.get("url").toString(), defaultDataSourceConfig.get("username").toString(), defaultDataSourceConfig.get("password").toString()));

        return dynamicDataSource;
    }

    @Bean
    public DBRouterConfig dbRouterConfig() {
        return new DBRouterConfig(dbCount, tbCount, routerKey);
    }


}
