package com.dtstack.flinkx.logminer.Util.models;

/**
 * Created by shejiewei on 2020/12/11.
 */

public class Column{

    private String owner;
    private String tableName;
    private String columnName;
    private Boolean nullable;
    private String dataType;
    private int dataLength;
    private int dataScale;
    private Boolean pkColumn;
    private Boolean uqColumn;


    public Column(String owner,String tableName,String columnName,Boolean nullable,String dataType,int dataLength,int dataScale,Boolean pkColumn,Boolean uqColumn){
        super();
        this.owner=owner;
        this.tableName=tableName;
        this.columnName=columnName;
        this.nullable=nullable;
        this.dataType=dataType;
        this.dataLength=dataLength;
        this.dataScale=dataScale;
        this.pkColumn=pkColumn;
        this.uqColumn=uqColumn;

    }

    public String getOwner(){
        return owner;
    }

    public void setOwner(String owner){
        this.owner=owner;
    }

    public String getTableName(){
        return tableName;
    }

    public void setTableName(String tableName){
        this.tableName=tableName;
    }

    public String getColumnName(){
        return columnName;
    }

    public void setColumnName(String columnName){
        this.columnName=columnName;
    }

    public Boolean getNullable(){
        return nullable;
    }

    public void setNullable(Boolean nullable){
        this.nullable=nullable;
    }

    public String getDataType(){
        return dataType;
    }

    public void setDataType(String dataType){
        this.dataType=dataType;
    }

    public int getDataLength(){
        return dataLength;
    }

    public void setDataLength(int dataLength){
        this.dataLength=dataLength;
    }

    public int getDataScale(){
        return dataScale;
    }

    public void setDataPrecision(int dataScale){
        this.dataScale=dataScale;
    }

    public Boolean getPkColumn(){
        return pkColumn;
    }

    public void setPkColumn(Boolean pkColumn){
        this.pkColumn=pkColumn;
    }
    
    public Boolean getUqColumn(){
        return uqColumn;
    }

    public void setUqColumn(Boolean uqColumn){
        this.uqColumn=uqColumn;
    }    


}