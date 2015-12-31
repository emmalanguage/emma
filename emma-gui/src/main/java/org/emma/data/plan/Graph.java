package org.emma.data.plan;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;

public class Graph {
    public ArrayList<Node> nodes = new ArrayList<>();
    public ArrayList<Edge> edges = new ArrayList<>();
    public String name = "";

    public Graph(Plan plan) {
        this.name = plan.name;
        String json = plan.plan;
        if (!json.isEmpty()) {
            parseNode(new JsonParser().parse(json).getAsJsonObject());
        }
    }

    private void parseNode(JsonObject object) {
        Integer id = object.get("id").getAsInt();
        String label = object.get("label").getAsString();

        Node n = new Node();
        n.setLabel(label);
        n.setId(id);

        if (object.has("type")) {
            NodeType type = NodeType.valueOf(object.get("type").getAsString());
            n.setType(type);
        }

        n.setTooltip(object.get("tooltip").getAsString());

        nodes.add(n);

        JsonArray parents = object.getAsJsonArray("parents");
        if (parents.size() > 0) {
            for (JsonElement parent : parents) {
                JsonObject parentObject = parent.getAsJsonObject();
                Edge e = new Edge(parentObject.get("id").getAsInt(),id);
                edges.add(e);
                parseNode(parentObject);
            }
        }
    }
}
