/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emma.data.plan;

import scala.collection.Iterator;
import scala.collection.immutable.List;
import scala.collection.immutable.Set;

import java.util.ArrayList;

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
