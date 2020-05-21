/**
 *
 * Title:        ExampleDataEngineQuery.java - Data engine examples for project.
 *
 * Description:  Examples on how to use the data engine. Three groups of 
 *               examples are given:
 *          
 *               1. demo - directly use data set to query/access data.
 *               2. query - general-purpose methods to work with data sets.
 *               3. test - illustrate using general purpose methods
 * 
 *               The static method "demo" shows how to do it "all in one" 
 *               method to use the data engine, dump information, and access
 *               a data set. 
 *
 * Copyright:    Copyright © (c) 2020 Neurodiversity In The Workplace (NITW)
 *
 * Development:  Developed and written by the contributions from Sean Gill,
 *               Joseph Riddle, and Christine P. Chai, Ph.D.
 *
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 * 
 */
package org.nitw.project;

//reference internal data structures from Java collections
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ExampleDataEngineQuery {

    private DataEngine dEng;

    public ExampleDataEngineQuery() {
        
        this.dEng = new DataEngine(false);
        this.dEng.loadData();

    }//end ExampleDataEngineQuery

    public void demoQueryDataSetAttribute() {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (this.dEng.hasDataSetName("CountyMedianIncome")) {

            //get reference to data set
            dataSet = this.dEng.getDataSetByName("CountyMedianIncome");
        } else {
            System.err.println("Data set not found in data engine!");
            return;
        }//end if

        //iterate through attribute in each record/map
        //print each value in the data set
        for (Map<String, String> record : dataSet) {

            String val = record.get("B06011_001E");  //B06011_001E is Census Bureau name for median income attribute by county
            
            //get attribute of map/record that is non-null, actually has a value
            if (val != null) {
                System.out.printf("Median Income: %s%n", val);
            }//end if

        }//end for

        System.out.println();

    }//end demoQueryDataSetAttribute

    public void demoQueryDataSetAttributeMinMax() {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        Integer min = Integer.MAX_VALUE;
        Integer max = Integer.MIN_VALUE;

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (this.dEng.hasDataSetName("CountyMedianIncome")) {

            //get reference to data set
            dataSet = this.dEng.getDataSetByName("CountyMedianIncome");
        } else {
            System.err.println("Data set not found in data engine!");
            return;
        }//end if

        //iterate through attribute in each record/map
        //print each value in the data set
        for (Map<String, String> record : dataSet) {

            String val = record.get("B06011_001E");  //B06011_001E is Census Bureau name for median income attribute by county
            
            //get attribute of map/record that is non-null, actually has a value
            if (val != null) {

                //System.out.printf("Median Income: %s%n", countyVal);
                Integer intVal = Integer.valueOf(val);

                if (intVal < min) {
                    min = intVal;
                } else if (intVal > max) {
                    max = intVal;
                }//end if

            }//end if

        }//end for

        System.out.printf("Maximum median income: %d%n", max);
        System.out.printf("Minimum median income: %d%n", min);

        System.out.println();

    }//end demoQueryDataSetAttributeMinMax

    public void demoQueryDataSetAttributeMinMaxCountyName() {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        Integer min = Integer.MAX_VALUE;
        int minRow = -1;
        Integer max = Integer.MIN_VALUE;
        int maxRow = -1;

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (this.dEng.hasDataSetName("CountyMedianIncome")) {

            //get reference to data set
            dataSet = this.dEng.getDataSetByName("CountyMedianIncome");
        } else {
            System.err.println("Data set not found in data engine!");
            return;
        }//end if

        int rowCounter = 0;

        //iterate through attribute in each record/map
        //print each value in the data set
        for (Map<String, String> record : dataSet) {

            String val = record.get("B06011_001E");  //B06011_001E is Census Bureau name for median income attribute by county
            
//get attribute of map/record that is non-null, actually has a value
            if (val != null) {

                Integer intVal = Integer.valueOf(val);

                if (intVal < min) {
                    min = intVal;
                    minRow = rowCounter;
                } else if (intVal > max) {
                    max = intVal;
                    maxRow = rowCounter;
                }//end if

            }//end if

            rowCounter++;
        }//end for

        System.out.printf("Maximum median income: %d at row:%d %n", max, maxRow);
        System.out.printf("Minimum median income: %d at row:%d %n", min, minRow);

        System.out.printf("%n%n");

        //get NAME using row index of minimum, maximum value
        String minCountyName = dataSet.get(minRow).get("NAME");
        String maxCountyName = dataSet.get(maxRow).get("NAME");

        System.out.printf("Min County Name: %s%n", minCountyName);
        System.out.printf("Max County Name: %s%n", maxCountyName);

        System.out.println();

    }//end demoQueryDataSetAttributeMinMaxCountyName

    public void demoQueryDataSetAttributeNullValue() {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (this.dEng.hasDataSetName("CountyMedianIncome")) {

            //get reference to data set
            dataSet = this.dEng.getDataSetByName("CountyMedianIncome");
        } else {
            System.err.println("Data set not found in data engine!");
            return;
        }//end if

        //print each value in the data set
        for (Map<String, String> record : dataSet) {

            String val = record.get("B06011_001E");  //B06011_001E is Census Bureau name for median income attribute by county
            
//get attribute of map/record that is non-null, actually has a value
            if (val == null) {
                System.out.printf("Null value/no datum for county, state: %s%n", record.get("NAME"));
            }//end if

        }//end for

        System.out.println();

    }//end demoQueryDataSetAttributeNullValue

    public void demoQueryDataSetAttributeCountyState() {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (this.dEng.hasDataSetName("CountyList")) {

            //get reference to data set
            dataSet = this.dEng.getDataSetByName("CountyList");
        } else {
            System.err.println("Data set not found in data engine!");
            return;
        }//end if

        //iterate through attribute in each record/map
        //print each value in the data set
        for (Map<String, String> record : dataSet) {

            String countyVal = record.get("COUNTY");
            String stateVal = record.get("STATE");

            //get attribute of map/record that is non-null, actually has a value
            if (countyVal != null) {

                System.out.printf("%s ", stateVal);

                //remove "County" "Borough" "Census Area" "Parish" "Census" from county name
                String countyName[] = countyVal.split(" ");

                for (int idx = 0; idx < countyName.length - 1; idx++) {
                    if (!countyName[idx].equalsIgnoreCase("Census")) {
                        System.out.printf("%s ", countyName[idx]);
                    }//end if
                }//end for
                
                System.out.println();

            }//end if

        }//end for

        System.out.println();

    }//end demoQueryDataSetAttributeCountyState

    public void demoQueryDataSetAttributeInRange(int lowerValue, int upperValue) {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (this.dEng.hasDataSetName("CountyMedianIncome")) {

            //get reference to data set
            dataSet = this.dEng.getDataSetByName("CountyMedianIncome");
        } else {
            System.err.println("Data set not found in data engine!");
            return;
        }//end if

        //iterate through attribute in each record/map
        //print each value in the data set
        for (Map<String, String> record : dataSet) {

            String val = record.get("B06011_001E");  //B06011_001E is Census Bureau name for median income attribute by county
            
//get attribute of map/record that is non-null, actually has a value
            if (val != null) {

                Integer intVal = Integer.valueOf(val);

                if (intVal >= lowerValue && intVal <= upperValue) {
                    System.out.printf("Median Income: %s%n", val);
                }//end if

            }//end if

        }//end for

        System.out.println();

    }//end demoQueryDataSetAttributeInRange

    public void demoQueryDataSetAttributeInRangeGetOtherAttribute(int lowerValue, int upperValue) {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (this.dEng.hasDataSetName("CountyMedianIncome")) {

            //get reference to data set
            dataSet = this.dEng.getDataSetByName("CountyMedianIncome");
        } else {
            System.err.println("Data set not found in data engine!");
            return;
        }//end if

        int rowCounter = 0;

        //iterate through attribute in each record/map
        //print each value in the data set
        for (Map<String, String> record : dataSet) {

            //B06011_001E is Census Bureau name for median income attribute by county
            String val = record.get("B06011_001E");

            //get attribute of map/record that is non-null, actually has a value
            if (val != null) {

                Integer intVal = Integer.valueOf(val);

                if (intVal >= lowerValue && intVal <= upperValue) {
                    System.out.printf("Median Income: %s ", val);

                    //get other attribute in data set
                    System.out.printf("County, State: %s ", record.get("NAME"));
                    System.out.println();
                }//end if

            }//end if

            rowCounter++;
        }//end for

        System.out.println();

    }//end demoQueryDataSetAttributeInRangeGetOtherAttribute

    public void demoQueryDataSetAttributeInRangeGetOtherAttributeDataSet(int lowerValue, int upperValue) {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet0 = null;

        //check if data set exists in data engine
        if (this.dEng.hasDataSetName("CountyMedianIncome")) {

            //get reference to data set
            dataSet0 = this.dEng.getDataSetByName("CountyMedianIncome");
        } else {
            System.err.println("Data set not found in data engine!");
            return;
        }//end if

        List<Map<String, String>> dataSet1 = null;

        //check if data set exists in data engine
        if (this.dEng.hasDataSetName("CountyPopulationTax")) {

            //get reference to data set
            dataSet1 = this.dEng.getDataSetByName("CountyPopulationTax");
        } else {
            System.err.println("Data set not found in data engine!");
            return;
        }//end if

        int rowCounter = 0;

        //iterate through attribute in each record/map
        //print each value in the data set
        for (Map<String, String> record : dataSet0) {

            //B06011_001E is Census Bureau name for median income attribute by county
            String val = record.get("B06011_001E");

            //get attribute of map/record that is non-null, actually has a value
            if (val != null) {

                Integer intVal = Integer.valueOf(val);

                if (intVal >= lowerValue && intVal <= upperValue) {
                    System.out.printf("CountyMedianIncome.Median Income: %s ", val);

                    //get other attribute in data set
                    String attrName = record.get("NAME");
                    String[] attrNames = attrName.split(",");
                    String attrNameCounty = attrNames[0];
                    String attrNameState = attrNames[1];
                    //System.out.printf("Name County: %s Name State: %s ", attrNameCounty, attrNameState);
                    //System.out.printf("County, State: %s ", record.get("NAME"));

                    //find other record/row with same county name, state name
                    for (int idx = 0; idx < dataSet1.size(); idx++) {

                        Map<String, String> rec = dataSet1.get(idx);
                        String attrVal0 = rec.get("NAME");

                        //check for match or join at index on county name and state name
                        if (attrVal0.indexOf(attrNameCounty) >= 0 && attrVal0.indexOf(attrNameState) >= 0) {
                            System.out.printf("| CountyPopulationTax.NAME: %s ", attrVal0);

                            String attrVal1 = rec.get("Local Tax Rate");
                            System.out.printf("| CountyPopulationTax.Local Tax Rate: %s ", attrVal1);

                        }//end if
                    }//end for

                    System.out.println();

                }//end if

            }//end if

            rowCounter++;
        }//end for

        System.out.println();

    }//end demoQueryDataSetAttributeInRangeGetOtherAttributeDataSet

    public void testQueryDataSetByAttrNameValueEQ() {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        final String DATA_SET = "StateTaxRates";
        final String ATTR_NAME = "State";
        final String[] ATTR_VAL = new String[]{"Texas", "Alaska"};
        List<Object[]> list = null;

        list = queryDataSetByAttrNameValueEQ(this.dEng, DATA_SET, ATTR_NAME, ATTR_VAL);

        //dump data for index, attribute value
        for (Object[] obj : list) {
            String attrValue = (String) obj[0];
            Integer attrIndex = (Integer) obj[1];
            System.out.printf("Index: %d Value: %s%n", attrIndex, attrValue);
        }//end for

    }//end testQueryDataSetbyAttrNameValueEQ

    public void testQueryDataSetByAttrNameValueNE() {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        final String DATA_SET = "StateTaxRates";
        final String ATTR_NAME = "State";
        final String ATTR_VAL = "Maine"; //remember the Maine!
        List<Object[]> list = null;

        list = queryDataSetByAttrNameValueNE(this.dEng, DATA_SET, ATTR_NAME, ATTR_VAL);

        //dump data for index, attribute value
        for (Object[] obj : list) {
            String attrValue = (String) obj[0];
            Integer attrIndex = (Integer) obj[1];

            System.out.printf("Index: %d Value: %s%n", attrIndex, attrValue);
        }//end for

    }//end testQueryDataSetbyAttrNameValueNE

    public void testQueryDataSetByAttrNameValueGE() {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        final String DATA_SET = "StateTaxRates";
        final String ATTR_NAME = "State";
        final String ATTR_VAL = "Maine"; //remember the Maine!
        List<Object[]> list = null;

        list = queryDataSetByAttrNameValueGE(this.dEng, DATA_SET, ATTR_NAME, ATTR_VAL);

        //dump data for index, attribute value
        for (Object[] obj : list) {
            String attrValue = (String) obj[0];
            Integer attrIndex = (Integer) obj[1];

            System.out.printf("Index: %d Value: %s%n", attrIndex, attrValue);
        }//end for

    }//end testQueryDataSetbyAttrNameValueGE

    public void testQueryDataSetByAttrNameValueGT() {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        final String DATA_SET = "StateTaxRates";
        final String ATTR_NAME = "State";
        final String ATTR_VAL = "Maine"; //remember the Maine!
        List<Object[]> list = null;

        list = queryDataSetByAttrNameValueGT(this.dEng, DATA_SET, ATTR_NAME, ATTR_VAL);

        //dump data for index, attribute value
        for (Object[] obj : list) {
            String attrValue = (String) obj[0];
            Integer attrIndex = (Integer) obj[1];

            System.out.printf("Index: %d Value: %s%n", attrIndex, attrValue);
        }//end for

    }//end testQueryDataSetbyAttrNameValueGT

    public void testQueryDataSetByAttrNameValueLT() {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        final String DATA_SET = "StateTaxRates";
        final String ATTR_NAME = "State";
        final String ATTR_VAL = "Maine"; //remember the Maine!
        List<Object[]> list = null;

        list = queryDataSetByAttrNameValueLT(this.dEng, DATA_SET, ATTR_NAME, ATTR_VAL);

        //dump data for index, attribute value
        for (Object[] obj : list) {
            String attrValue = (String) obj[0];
            Integer attrIndex = (Integer) obj[1];

            System.out.printf("Index: %d Value: %s%n", attrIndex, attrValue);
        }//end for

    }//end testQueryDataSetbyAttrNameValueLT

    public void testQueryDataSetByAttrNameValueLE() {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        final String DATA_SET = "StateTaxRates";
        final String ATTR_NAME = "State";
        final String ATTR_VAL = "Maine"; //remember the Maine!
        List<Object[]> list = null;

        list = queryDataSetByAttrNameValueLE(this.dEng, DATA_SET, ATTR_NAME, ATTR_VAL);

        //dump data for index, attribute value
        for (Object[] obj : list) {
            String attrValue = (String) obj[0];
            Integer attrIndex = (Integer) obj[1];

            System.out.printf("Index: %d Value: %s%n", attrIndex, attrValue);
        }//end for

    }//end testQueryDataSetbyAttrNameValueLE

    //query other attribute name, value by index in same dataSet   queryDataSetRowIndex(engine, dataset, list<Object[]>, attributeName)
    public void testQueryAllDataSetByAttriNameALL() {

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        final String DATA_SET = "StateExports";
        final String ATTR_NAME = "Tons (thousands)";

        List<Object[]> list = null;

        list = queryDataSetByAttributeNameALL(this.dEng, DATA_SET, ATTR_NAME);

        //dump data for index, attribute value
        for (Object[] obj : list) {
            String attrValue = (String) obj[0];
            Integer attrIndex = (Integer) obj[1];

            System.out.printf("Index: %d Value: %s%n", attrIndex, attrValue);
        }//end for

    }//end testQueryAllDataSetByAttriNameALL 

    public void testGetDataSetHeaders() {

        Set<String> dataSetNames = this.dEng.getDataSetNames();

        for (String dataSetName : dataSetNames) {

            Set<String> headers = ExampleDataEngineQuery.listDataSetHeaders(dEng, dataSetName);

            System.out.printf("Data Set: %s %n%n", dataSetName);

            for (String header : headers) {
                System.out.printf("  Header: %s%n", header);
            }//end for

            System.out.println();

        }//end for

    }//end testGetDataSetHeaders

    public static final Set<String> listDataSetHeaders(final DataEngine dEng, final String nameDataSet) {

        Set<String> nameSet = null;

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (dEng.hasDataSetName(nameDataSet)) {

            //get reference to data set
            dataSet = dEng.getDataSetByName(nameDataSet);
        } else {
            System.err.printf("Data set: %s not found in data engine!", nameDataSet);
            return nameSet;
        }//end if

        nameSet = dataSet.get(0).keySet();

        return nameSet;

    }//end listDataSetHeaders

    public static final List<Object[]> queryDataSetByAttributeNameALL(final DataEngine dEng, final String nameDataSet, String nameAttr) {

        List<Object[]> list = new ArrayList<>(); //Object[0] = attributeValue.String, Object[1] attributeRowIndex.Integer

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (dEng.hasDataSetName(nameDataSet)) {

            //get reference to data set
            dataSet = dEng.getDataSetByName(nameDataSet);
        } else {
            System.err.printf("Data set: %s not found in data engine!", nameDataSet);
            return list;
        }//end if

        int rowCounter = 0;

        //iterate through attribute in each record/map
        for (Map<String, String> record : dataSet) {

            String attrVal = record.get(nameAttr);

            list.add(new Object[]{attrVal, rowCounter});

            rowCounter++;
        }//end for

        return list;

    }//end queryDataSetByAttributeNameALL

    public static final List<Object[]> queryDataSetByAttrNameValueEQ(final DataEngine dEng, final String nameDataSet, String nameAttr, String... valAttr) {

        List<Object[]> list = new ArrayList<>(); //Object[0] = attributeValue.String, Object[1] attributeRowIndex.Integer

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (dEng.hasDataSetName(nameDataSet)) {

            //get reference to data set
            dataSet = dEng.getDataSetByName(nameDataSet);
        } else {
            System.err.printf("Data set: %s not found in data engine!", nameDataSet);
            return list;
        }//end if

        int rowCounter = 0;

        //iterate through attribute in each record/map
        for (Map<String, String> record : dataSet) {

            String attrVal = record.get(nameAttr);

            for (int idx = 0; idx < valAttr.length; idx++) {
                if (attrVal.compareTo(valAttr[idx]) == 0) {
                    list.add(new Object[]{attrVal, rowCounter});
                    //System.out.printf("attrVal: %s == valAttr: %s %n", attrVal, valAttr);
                }//end if
            }
            rowCounter++;
        }//end for

        return list;

    }//end queryDataSetByAttrNameValueEQ

    public static final List<Object[]> queryDataSetByAttrNameValueNE(final DataEngine dEng, final String nameDataSet, String nameAttr, String... valAttr) {

        List<Object[]> list = new ArrayList<>(); //Object[0] = attributeValue.String, Object[1] attributeRowIndex.Integer

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (dEng.hasDataSetName(nameDataSet)) {

            //get reference to data set
            dataSet = dEng.getDataSetByName(nameDataSet);
        } else {
            System.err.printf("Data set: %s not found in data engine!", nameDataSet);
            return list;
        }//end if

        int rowCounter = 0;
        //iterate through attribute in each record/map
        for (Map<String, String> record : dataSet) {

            String attrVal = record.get(nameAttr);

            for (int idx = 0; idx < valAttr.length; idx++) {
                if (attrVal.compareTo(valAttr[idx]) != 0) {
                    list.add(new Object[]{attrVal, rowCounter});
                    //System.out.printf("attrVal: %s == valAttr: %s %n", attrVal, valAttr);
                }//end if
            }
            rowCounter++;
        }//end for

        return list;

    }//end queryDataSetByAttrNameValueNE

    public static final List<Object[]> queryDataSetByAttrNameValueGT(final DataEngine dEng, final String nameDataSet, String nameAttr, String... valAttr) {

        List<Object[]> list = new ArrayList<>(); //Object[0] = attributeValue.String, Object[1] attributeRowIndex.Integer

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (dEng.hasDataSetName(nameDataSet)) {

            //get reference to data set
            dataSet = dEng.getDataSetByName(nameDataSet);
        } else {
            System.err.printf("Data set: %s not found in data engine!", nameDataSet);
            return list;
        }//end if

        int rowCounter = 0;
        
        //iterate through attribute in each record/map
        for (Map<String, String> record : dataSet) {

            String attrVal = record.get(nameAttr);

            for (int idx = 0; idx < valAttr.length; idx++) {
                if (attrVal.compareTo(valAttr[idx]) > 0) {
                    list.add(new Object[]{attrVal, rowCounter});
                    //System.out.printf("attrVal: %s == valAttr: %s %n", attrVal, valAttr);
                }//end if
            }
            rowCounter++;
        }//end for

        return list;

    }//end queryDataSetByAttrNameValueGT

    public static final List<Object[]> queryDataSetByAttrNameValueGE(final DataEngine dEng, final String nameDataSet, String nameAttr, String... valAttr) {

        List<Object[]> list = new ArrayList<>(); //Object[0] = attributeValue.String, Object[1] attributeRowIndex.Integer

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (dEng.hasDataSetName(nameDataSet)) {

            //get reference to data set
            dataSet = dEng.getDataSetByName(nameDataSet);
        } else {
            System.err.printf("Data set: %s not found in data engine!", nameDataSet);
            return list;
        }//end if

        int rowCounter = 0;
        
        //iterate through attribute in each record/map
        for (Map<String, String> record : dataSet) {

            String attrVal = record.get(nameAttr);

            for (int idx = 0; idx < valAttr.length; idx++) {
                if (attrVal.compareTo(valAttr[idx]) >= 0) {
                    list.add(new Object[]{attrVal, rowCounter});
                    //System.out.printf("attrVal: %s == valAttr: %s %n", attrVal, valAttr);
                }//end if
            }
            rowCounter++;
        }//end for

        return list;

    }//end queryDataSetByAttrNameValueGE

    public static final List<Object[]> queryDataSetByAttrNameValueLT(final DataEngine dEng, final String nameDataSet, String nameAttr, String... valAttr) {

        List<Object[]> list = new ArrayList<>(); //Object[0] = attributeValue.String, Object[1] attributeRowIndex.Integer

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (dEng.hasDataSetName(nameDataSet)) {

            //get reference to data set
            dataSet = dEng.getDataSetByName(nameDataSet);
        } else {
            System.err.printf("Data set: %s not found in data engine!", nameDataSet);
            return list;
        }//end if

        int rowCounter = 0;
        
        //iterate through attribute in each record/map
        for (Map<String, String> record : dataSet) {

            String attrVal = record.get(nameAttr);

            for (int idx = 0; idx < valAttr.length; idx++) {
                if (attrVal.compareTo(valAttr[idx]) < 0) {
                    list.add(new Object[]{attrVal, rowCounter});
                    //System.out.printf("attrVal: %s == valAttr: %s %n", attrVal, valAttr);
                }//end if
            }
            rowCounter++;
        }//end for

        return list;

    }//end queryDataSetByAttrNameValueLT

    public static final List<Object[]> queryDataSetByAttrNameValueLE(final DataEngine dEng, final String nameDataSet, String nameAttr, String... valAttr) {

        List<Object[]> list = new ArrayList<>(); //Object[0] = attributeValue.String, Object[1] attributeRowIndex.Integer

        System.out.printf("----------%nMethod: %s%n%n", Thread.currentThread().getStackTrace()[1].getMethodName());

        List<Map<String, String>> dataSet = null;

        //check if data set exists in data engine
        if (dEng.hasDataSetName(nameDataSet)) {

            //get reference to data set
            dataSet = dEng.getDataSetByName(nameDataSet);
        } else {
            System.err.printf("Data set: %s not found in data engine!", nameDataSet);
            return list;
        }//end if

        int rowCounter = 0;
        
        //iterate through attribute in each record/map
        for (Map<String, String> record : dataSet) {

            String attrVal = record.get(nameAttr);

            for (int idx = 0; idx < valAttr.length; idx++) {
                if (attrVal.compareTo(valAttr[idx]) <= 0) {
                    list.add(new Object[]{attrVal, rowCounter});
                    //System.out.printf("attrVal: %s == valAttr: %s %n", attrVal, valAttr);
                }//end if
            }
            rowCounter++;
        }//end for

        return list;

    }//end queryDataSetByAttrNameValueLE

    //load file data into engine data sets; then dump first 4-records of each
    //and list the data sets by name in descending sorted order. Then access
    //data set by name to get list of records/maps.
    public static void demo() {

        //create instance of data engine
        final DataEngine dEng = new DataEngine();

        try {

            //load or import data from external files
            dEng.loadData();

            //dump first four records of each data set
            dEng.dumpDataSets(4);

            //list the names of data sets in sorted descending order
            System.out.printf("Listing Data Set by Name:%n%n");

            List<String> list = new ArrayList<String>(dEng.getDataSetNames());
            Collections.sort(list);

            for (String dataSetName : list) {
                System.out.printf("  %s%n", dataSetName);
            }//end for

            System.out.printf("%n%n");

            //get data set by name directly 
            List< Map<String, String>> dataSet = dEng.getDataSetByName("CountyUnemployment");

            
        } catch (Exception ex) {
            System.err.printf("Error: %s%n", ex.getMessage());
            ex.printStackTrace();
        }//end try

    }//end demo

    //call all 25-methods to demonstrate use of data engine
    public static void main(String[] args) {

        ExampleDataEngineQuery edeq = new ExampleDataEngineQuery();

        edeq.demoQueryDataSetAttribute();
        edeq.demoQueryDataSetAttributeMinMax();
        edeq.demoQueryDataSetAttributeMinMaxCountyName();
        edeq.demoQueryDataSetAttributeNullValue();
        edeq.demoQueryDataSetAttributeCountyState();
        edeq.demoQueryDataSetAttributeInRange(30_000, 40_000);
        edeq.demoQueryDataSetAttributeInRangeGetOtherAttribute(30_000, 40_000);
        edeq.demoQueryDataSetAttributeInRangeGetOtherAttributeDataSet(30_000, 40_000);

        edeq.testQueryAllDataSetByAttriNameALL();
        edeq.testQueryDataSetByAttrNameValueEQ();
        edeq.testQueryDataSetByAttrNameValueNE();
        edeq.testQueryDataSetByAttrNameValueGE();
        edeq.testQueryDataSetByAttrNameValueGT();
        edeq.testQueryDataSetByAttrNameValueLE();
        edeq.testQueryDataSetByAttrNameValueLT();
        edeq.testGetDataSetHeaders();

        demo();
        
        System.exit(0);

    }//end main

}//end class ExampleDataEngineQuery
