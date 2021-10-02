package com.gallop.connect;

import com.gallop.connect.logminer.source.model.Offset;
import com.gallop.connect.logminer.source.model.Table;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * author gallop
 * date 2021-09-23 7:50
 * Description:
 * Modified By:
 */
public class ExecutorServiceTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newCachedThreadPool();
        final Map<Table, Offset> innerState = new HashMap<>();
        Table t = new Table("orcl","usercenter","test_user");
        Offset o = new Offset(123l,122l,"aaw+123");
        innerState.put(t,o);
        Callable<Map> queryCall = new Callable<Map>() {
            public Map call() throws SQLException {
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("execu call inner.....");
                return innerState;
            }
        };

        Future<Map> future = executorService.submit(queryCall);
        Map<Table, Offset> newState = (Map<Table, Offset>)future.get();
        System.out.println("state:"+newState);
        System.out.println("after call submit....");
    }
}
