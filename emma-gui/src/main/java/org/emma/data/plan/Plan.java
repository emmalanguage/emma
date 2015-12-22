package org.emma.data.plan;

import scala.collection.Iterator;
import scala.collection.immutable.List;
import scala.collection.immutable.Set;

import java.util.ArrayList;

/**
 * Created by Andi on 18.11.2015.
 */
public class Plan {
    String name;
    String plan;
    private Object comprehensions;

    public Plan(String name, String plan, Object comprehensions) {
        this.name = name;
        this.plan = plan;
        this.comprehensions = comprehensions;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPlan() {
        return plan;
    }

    public void setPlan(String plan) {
        this.plan = plan;
    }

    public ArrayList<ArrayList<Integer>> getComprehensions() {
        ArrayList<ArrayList<Integer>> list = new ArrayList<>();
        Set s = (scala.collection.immutable.Set)comprehensions;
        List l = s.toList();
        Iterator i = l.iterator();
        while (i.hasNext()) {
            scala.Tuple2 tuple = (scala.Tuple2)i.next();
            ArrayList<Integer> box = new ArrayList<>();
            box.add((Integer)tuple._1());
            box.add((Integer)tuple._2());
            list.add(box);
        }

        return list;
    }

    public void setComprehensions(Object comprehensions) {
        this.comprehensions = comprehensions;
    }
}
