package com.gallop.connect;

import com.gallop.connect.logminer.source.model.Offset;
import com.gallop.connect.logminer.source.model.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * author gallop
 * date 2021-09-23 7:11
 * Description:
 * Modified By:
 */
public class OffsetTest {
    public static void main(String[] args) {
        Map<Table, Offset> state1 = new HashMap<>();
        Table t = new Table("orcl","usercenter","test_user");
        Offset o = new Offset(123l,122l,"aaw+123");
        state1.put(t,o);

        Map<Table, Offset> state2 = new HashMap<>();
        Table t2 = new Table("orcl2","usercenter","test_user");
        Offset o2 = new Offset(123l,122l,"aaw+123");
        state2.put(t2,o2);

        System.out.println("result=="+state1.equals(state2));

    }


}
