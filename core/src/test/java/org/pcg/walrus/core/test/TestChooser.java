package org.pcg.walrus.core.test;

import org.junit.Test;

import java.util.*;

public class TestChooser {

    @Test
    public void testCostChoose() {
        Chosser c1 = new Chosser();
        c1.rows = 10;
        Chosser c2 = new Chosser();
        c2.rows = 20;
        List<Chosser> list = new ArrayList<Chosser>();
        list.add(c1);
        list.add(c2);
        Collections.sort(list, new Comparator<Chosser>() {
            @Override
            public int compare(Chosser o1, Chosser o2) {
                return (int) (c2.rows - c1.rows);
            }
        });
        assert list.get(0).rows == 10;
    }

    private class Chosser {
        public long rows;
    }
}
