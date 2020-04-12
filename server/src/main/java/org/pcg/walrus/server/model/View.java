package org.pcg.walrus.server.model;

import org.json.simple.JSONObject;

/**
 * {meta.view}
 */
public class View {

    private String name;
    private JSONObject view; // view logic

    public View(String name) {
        setName(name);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public JSONObject getView() {
        return view;
    }

    public void setView(JSONObject view) {
        this.view = view;
    }
}
