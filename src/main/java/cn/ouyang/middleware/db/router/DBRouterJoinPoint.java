package cn.ouyang.middleware.db.router;

import cn.ouyang.middleware.db.router.annotation.DBRouter;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.Method;

/**
 * 数据路由切面
 * 在这个类中，根据key，通过斐波拉奇散列进行hash计算，确定路由的库表
 */
@Aspect
public class DBRouterJoinPoint {

    private Logger logger = LoggerFactory.getLogger(DBRouterJoinPoint.class);

    @Autowired
    private DBRouterConfig dbRouterConfig;

    @Pointcut("@annotation(cn.ouyang.middleware.db.router.annotation.DBRouter)")
    public void aopPoint(){
    }

    @Around("aopPoint() && @annotation(dbRouter)")
    public Object doRouter(ProceedingJoinPoint jp, DBRouter dbRouter) throws Throwable{
        String dbKey = dbRouter.key();
        if(StringUtils.isBlank(dbKey) && StringUtils.isBlank(dbRouterConfig.getRouterKey())){
            throw new RuntimeException("annotation DBRouter key is null！");
        }
        dbKey = StringUtils.isNotBlank(dbKey) ? dbKey : dbRouterConfig.getRouterKey();

        //计算路由
        String dbKeyAttr = getAttrValue(dbKey, jp.getArgs());
        int size = dbRouterConfig.getDbCount() * dbRouterConfig.getTbCount();

        int idx = (size -1) & (dbKeyAttr.hashCode() ^(dbKeyAttr.hashCode() >>> 16));

        // 库表索引
        int dbIdx = idx / dbRouterConfig.getTbCount() + 1;
        int tbIdx = idx - dbRouterConfig.getTbCount() * (dbIdx - 1);

        // 设置到 ThreadLocal
        DBContextHolder.setDBKey(String.format("%02d", dbIdx));
        DBContextHolder.setTBKey(String.format("%03d", tbIdx));
        logger.info("数据库路由 method：{} dbIdx：{} tbIdx：{}", getMethod(jp).getName(), dbIdx, tbIdx);

        // 返回结果
        try {
            return jp.proceed();
        } finally {
            //清理ThreadLocal
            DBContextHolder.clearDBKey();
            DBContextHolder.clearTBKey();
        }
    }

    private Method getMethod(JoinPoint jp) throws NoSuchMethodException {
        Signature sig = jp.getSignature();
        MethodSignature methodSignature = (MethodSignature) sig;
        return jp.getTarget().getClass().getMethod(methodSignature.getName(), methodSignature.getParameterTypes());
    }

    private String getAttrValue(String attrName,Object[] args){
        if (1 == args.length){
            return args[0].toString();
        }

        String filedValue = null;
        for (Object arg : args) {
            try{
                if (StringUtils.isNotBlank(filedValue)){
                    break;
                }
                filedValue = BeanUtils.getProperty(args,attrName);
            }catch (Exception e){
                logger.error("获取路由属性值失败 attr：{}",attrName,e);
            }
        }
        return filedValue;
    }


}
