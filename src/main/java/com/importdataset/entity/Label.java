package com.importdataset.entity;

import java.io.Serializable;

/**
 * 作者：chenhao
 * 日期：2020-02-29 14:06
 **/
public class Label implements Serializable {
    private Long id;

    private String dsourceid;

    private String labelid;

    private String labelname;

    private String tablename;

    private String newid;

    private String newname;

    private String remark;

    private Long dsabelid;

    private String datatype;

    private Integer timestampif;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDsourceid() {
        return dsourceid;
    }

    public void setDsourceid(String dsourceid) {
        this.dsourceid = dsourceid;
    }

    public String getLabelid() {
        return labelid;
    }

    public void setLabelid(String labelid) {
        this.labelid = labelid;
    }

    public String getLabelname() {
        return labelname;
    }

    public void setLabelname(String labelname) {
        this.labelname = labelname;
    }

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public String getNewid() {
        return newid;
    }

    public void setNewid(String newid) {
        this.newid = newid;
    }

    public String getNewname() {
        return newname;
    }

    public void setNewname(String newname) {
        this.newname = newname;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public Long getDsabelid() {
        return dsabelid;
    }

    public void setDsabelid(Long dsabelid) {
        this.dsabelid = dsabelid;
    }

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    public Integer getTimestampif() {
        return timestampif;
    }

    public void setTimestampif(Integer timestampif) {
        this.timestampif = timestampif;
    }

    @Override
    public String toString() {
        return "Label{" +
                "id=" + id +
                ", dsourceid='" + dsourceid + '\'' +
                ", labelid='" + labelid + '\'' +
                ", labelname='" + labelname + '\'' +
                ", tablename='" + tablename + '\'' +
                ", newid='" + newid + '\'' +
                ", newname='" + newname + '\'' +
                ", remark='" + remark + '\'' +
                ", dsabelid=" + dsabelid +
                ", datatype='" + datatype + '\'' +
                ", timestampif=" + timestampif +
                '}';
    }
}
