package com.importdataset.entity;

import java.io.Serializable;

public class CsvTableStructureModel implements Serializable {
    private String tablename;
    private String columns;
    private String tagNames;

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public String getTagNames() {
        return tagNames;
    }

    public void setTagNames(String tagNames) {
        this.tagNames = tagNames;
    }

    @Override
    public String toString() {
        return "CsvTableStructureModel{" +
                ", tablename='" + tablename + '\'' +
                ", columns='" + columns + '\'' +
                ", tagNames='" + tagNames + '\'' +
                '}';
    }
}