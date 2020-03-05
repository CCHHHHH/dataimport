package com.importdataset.entity;

/**
 * 作者：chenhao
 * 日期：2020-02-29 15:49
 **/
public class DataSource {
    private Long id;

    private String dsname;

    private String dstype;

    private String dsip;

    private String dsport;

    private String dsexample;

    private String dsloginid;

    private String dspwd;

    private boolean status;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDsname() {
        return dsname;
    }

    public void setDsname(String dsname) {
        this.dsname = dsname;
    }

    public String getDstype() {
        return dstype;
    }

    public void setDstype(String dstype) {
        this.dstype = dstype;
    }

    public String getDsip() {
        return dsip;
    }

    public void setDsip(String dsip) {
        this.dsip = dsip;
    }

    public String getDsport() {
        return dsport;
    }

    public void setDsport(String dsport) {
        this.dsport = dsport;
    }

    public String getDsexample() {
        return dsexample;
    }

    public void setDsexample(String dsexample) {
        this.dsexample = dsexample;
    }

    public String getDsloginid() {
        return dsloginid;
    }

    public void setDsloginid(String dsloginid) {
        this.dsloginid = dsloginid;
    }

    public String getDspwd() {
        return dspwd;
    }

    public void setDspwd(String dspwd) {
        this.dspwd = dspwd;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "DataSource{" +
                "id=" + id +
                ", dsname='" + dsname + '\'' +
                ", dstype='" + dstype + '\'' +
                ", dsip='" + dsip + '\'' +
                ", dsport='" + dsport + '\'' +
                ", dsexample='" + dsexample + '\'' +
                ", dsloginid='" + dsloginid + '\'' +
                ", dspwd='" + dspwd + '\'' +
                ", status=" + status +
                '}';
    }
}
