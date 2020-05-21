/**
 *
 * Title:        DataEngine.java - Data engine for project.
 *
 * Description:  Create DataEngine to import, and allow access, query various
 *               data values for project for 8-different data sets managed by
 *               the data engine.
 *
 *               The data sets are stored in external files of different types
 *               of formats (CSV, Excel, JSON, XML) of data from free public
 *               data from U.S. Census Bureau, Bureau of Labor Statistics, and
 *               Bureau of Economic Analysis.
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

//Apache POI - the Java API for Microsoft Documents https://poi.apache.org
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

//Jackson is a suite of data-processing tools for Java https://github.com/FasterXML/jackson
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

//XStream is a simple library to serialize objects to XML and back again. https://x-stream.github.io
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

//univocity-parsers is a collection of extremely fast and reliable parsers for Java. https://github.com/uniVocity/univocity-parsers
import com.univocity.parsers.csv.*;

//standard Java input-output 
import java.io.BufferedReader;
import java.io.Reader;
import java.io.File;
import java.io.FileInputStream;

//standard Java new input-output
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;

//standard Java data structures/collections
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataEngine {

    //auxiliary class used by XStream library to process XML into Java map data structure
    static class MapEntryConverter implements Converter {

        public boolean canConvert(Class cls) {
            return AbstractMap.class.isAssignableFrom(cls);
        }//end canConvert

        public void marshal(Object value, HierarchicalStreamWriter writer, MarshallingContext context) {

            AbstractMap map = (AbstractMap) value;
            for (Object obj : map.entrySet()) {
                Map.Entry entry = (Map.Entry) obj;
                writer.startNode(entry.getKey().toString());
                Object val = entry.getValue();
                if (null != val) {
                    writer.setValue(val.toString());
                }
                writer.endNode();
            }//end for

        }//end marshal

        public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {

            Map<String, String> map = new HashMap<String, String>();

            while (reader.hasMoreChildren()) {
                reader.moveDown();

                String key = reader.getNodeName(); // nodeName aka element's name
                String value = reader.getValue();
                map.put(key, value);

                reader.moveUp();
            }//end while

            return map;
        }//end unmarshal

    }//end class MapEntryConverter

    //files with external datum to import into engine
    public static final String fileNameCountyEmploymentWages 	= "US_St_Cn_Table_Workforce_Wages.xml"; 
    public static final String fileNameCountyList    			= "usa_county_list.csv";
    public static final String fileNameCountyMedianIncome 		= "Median_Income_County.json";
    public static final String fileNameCountyPopulationTax 		= "Population_By_County_State_County_Tax.csv";
    public static final String fileNameCountyUnemployment 		= "Unemployment_By_County.xlsx";
    public static final String fileNameStateExports 			= "Exports By State 2012.xlsx";
    public static final String fileNameStateTaxRates 			= "StateTaxRates.xlsx";

    private boolean echoImportFlag      = true;   //echo loading, record size, time
    private boolean dataEngineReadyFlag = false;  //indicate data engine is ready from data import

    /**
     * null constructor uses default settings
     */
    public DataEngine() {
    }//end constructor

    /**
     * constructor to take Boolean echoImportFlag
     *
     * @param echoImportFlag indicates to echo each data import from external
     * file of time, number of records.
     */
    public DataEngine(boolean echoImportFlag) {
        this.echoImportFlag = echoImportFlag;
    }//end constructor

    //data sets for each file 
    private List< Map<String, String>> dataSetCountyEmploymentWages; 
    private List< Map<String, String>> dataSetCountyList;
    private List< Map<String, String>> dataSetCountyMedianIncome;
    private List< Map<String, String>> dataSetCountyPopulationTax;
    private List< Map<String, String>> dataSetCountyUnemployment;
    private List< Map<String, String>> dataSetStateExports;
    private List< Map<String, String>> dataSetStateTaxRates;

    //data sets stored in list and map for collective access to all data sets
    private List< List< Map<String, String>>> dataSetList;
    private Map< String, List< Map<String, String>>> dataSetMap;

    //data set names
    private Set<String> dataSetNames;

    /**
     * Method that dumps map/record in each data set to a limit >= 1
     *
     * @param limit - how many records to dump as String from each data set
     */
    public final void dumpDataSets(final int limit) {

        if (!this.dataEngineReadyFlag) {
            throw new RuntimeException("Data engine not initialized with imported data from external files!");
        }//end if

        List<String> dataSets = new ArrayList<String>(this.getDataSetNames());
        Collections.sort(dataSets);

        for (String dataSet : dataSets) {

            List<Map<String, String>> list = this.dataSetMap.get(dataSet);

            System.out.printf("Data Set: %s%n", dataSet);

            for (int idx = 0; idx < limit; idx++) {

                Map<String, String> map = (Map<String, String>) list.get(idx);

                //System.out.printf("%s%n", map.toString().replace("{", "").replace("}", ""));
                Set<String> keySet = map.keySet();

                for (String key : keySet) {

                    String val = map.get(key);

                    System.out.printf("  key:'%s' => val:'%s' ", key, val);

                }//end for
                System.out.println();

            }//end for

            System.out.println();

        }//end for

    }//end dumpDataSets

    public static final List<Map<String, String>> importData(String filePath) {

        if (Files.notExists(Paths.get(filePath))) {
            System.err.printf("Error: File %s not found!%n%n", filePath);
            System.exit(1);
        }//end if

        List<Map<String, String>> list = null;

        String[] filePathExt = filePath.split("\\.");

        switch (filePathExt[1]) {

            case "csv":
                list = DataEngine.readCSV(filePath);
                break;
            case "json":
                list = DataEngine.readJSON(filePath);
                break;
            case "xlsx":
                list = DataEngine.readExcel(filePath);
                break;
            case "xml":
                list = DataEngine.readXML(filePath);
                break;
            default:
                System.err.printf("Error: Unknown file extension: %s%n!", filePathExt[1]);
                System.exit(1);
        }//end switch

        return list;

    }//end importData

    /**
     * Import or load datum from external data files into internal data
     * structures
     *
     */
    public final void loadData() {

        long totalTime = 0L, totalSize = 0L;

        long timeStart = 0L, timeClose = 0L;

        if (this.echoImportFlag) {
            System.out.printf("Starting Import Data Sets from Files.%n%n");
        }//end if

        try {

            if (this.echoImportFlag) {
                System.out.print("  Import County Population Tax CSV...    ");
            }//end if

            timeStart = System.currentTimeMillis();
            this.dataSetCountyPopulationTax = DataEngine.importData(DataEngine.fileNameCountyPopulationTax);
            timeClose = System.currentTimeMillis();

            if (this.echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", this.dataSetCountyPopulationTax.size(), (timeClose - timeStart));
            }//end if

            totalTime = totalTime + (timeClose - timeStart);
            totalSize = totalSize + this.dataSetCountyPopulationTax.size();

            if (this.echoImportFlag) {
                System.out.print("  Import USA County List CSV...          ");
            }//end if

            timeStart = System.currentTimeMillis();

            this.dataSetCountyList = DataEngine.importData(DataEngine.fileNameCountyList);

            timeClose = System.currentTimeMillis();

            if (this.echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", this.dataSetCountyList.size(), (timeClose - timeStart));
            }//end if

            totalTime = totalTime + (timeClose - timeStart);
            totalSize = totalSize + this.dataSetCountyList.size();

            if (this.echoImportFlag) {
                System.out.print("  Import County Unemployment Excel...    ");
            }//end if

            timeStart = System.currentTimeMillis();
            this.dataSetCountyUnemployment = DataEngine.importData(DataEngine.fileNameCountyUnemployment);
            timeClose = System.currentTimeMillis();

            if (this.echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", this.dataSetCountyPopulationTax.size(), (timeClose - timeStart));
            }//end if

            totalTime = totalTime + (timeClose - timeStart);
            totalSize = totalSize + this.dataSetCountyUnemployment.size();

            if (this.echoImportFlag) {
                System.out.print("  Import Exports By State Excel...       ");
            }//end if

            timeStart = System.currentTimeMillis();
            this.dataSetStateExports = DataEngine.importData(DataEngine.fileNameStateExports);
            timeClose = System.currentTimeMillis();

            if (this.echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", this.dataSetStateExports.size(), (timeClose - timeStart));
            }//end if

            totalTime = totalTime + (timeClose - timeStart);
            totalSize = totalSize + this.dataSetStateExports.size();

            if (this.echoImportFlag) {
                System.out.print("  Import State Tax Rates Excel...        ");
            }//end if

            timeStart = System.currentTimeMillis();
            this.dataSetStateTaxRates = DataEngine.importData(DataEngine.fileNameStateTaxRates);
            timeClose = System.currentTimeMillis();

            if (this.echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", this.dataSetStateTaxRates.size(), (timeClose - timeStart));
            }//end if

            totalTime = totalTime + (timeClose - timeStart);
            totalSize = totalSize + this.dataSetStateTaxRates.size();

            if (this.echoImportFlag) {
                System.out.print("  Import County Median Income JSON...    ");
            }//end if

            timeStart = System.currentTimeMillis();
            this.dataSetCountyMedianIncome = DataEngine.importData(DataEngine.fileNameCountyMedianIncome);
            timeClose = System.currentTimeMillis();

            if (this.echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", this.dataSetCountyMedianIncome.size(), (timeClose - timeStart));
            }//end if

            totalTime = totalTime + (timeClose - timeStart);
            totalSize = totalSize + this.dataSetCountyMedianIncome.size();

            if (this.echoImportFlag) {
                System.out.print("  Import County Employment Wages XML...  ");
            }//end if

            timeStart = System.currentTimeMillis();
            this.dataSetCountyEmploymentWages = DataEngine.importData(DataEngine.fileNameCountyEmploymentWages);
            timeClose = System.currentTimeMillis();

            if (this.echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", this.dataSetCountyEmploymentWages.size(), (timeClose - timeStart));
            }//end if

            totalTime = totalTime + (timeClose - timeStart);
            totalSize = totalSize + this.dataSetCountyEmploymentWages.size();

            if (this.echoImportFlag) {
                System.out.printf("Finished Import Data Sets from Files.%n%n");
                System.out.printf("Total %d-records imported in %d-mSec.%n%n", totalSize, totalTime);
            }//end if

            //create the list and map that allow collective/centralized access to all data sets
            this.dataSetList = new ArrayList<>(8);
            this.dataSetMap = new HashMap<>(8);

            this.dataSetList.add(this.dataSetCountyEmploymentWages);
            this.dataSetMap.put("CountyEmploymentWages", this.dataSetCountyEmploymentWages);

            this.dataSetList.add(this.dataSetCountyList);
            this.dataSetMap.put("CountyList", this.dataSetCountyList);

            this.dataSetList.add(this.dataSetCountyMedianIncome);
            this.dataSetMap.put("CountyMedianIncome", this.dataSetCountyMedianIncome);

            this.dataSetList.add(this.dataSetCountyPopulationTax);
            this.dataSetMap.put("CountyPopulationTax", this.dataSetCountyPopulationTax);

            this.dataSetList.add(this.dataSetCountyUnemployment);
            this.dataSetMap.put("CountyUnemployment", this.dataSetCountyUnemployment);

//            this.dataSetList.add(this.dataSetCountyWorkforceWages);
//            this.dataSetMap.put("CountyWorkforceWages", this.dataSetCountyWorkforceWages);
            this.dataSetList.add(this.dataSetStateExports);
            this.dataSetMap.put("StateExports", this.dataSetStateExports);

            this.dataSetList.add(this.dataSetStateTaxRates);
            this.dataSetMap.put("StateTaxRates", this.dataSetStateTaxRates);

            //initialize list of each data set name using the map
            this.dataSetNames = this.dataSetMap.keySet();

            this.dataEngineReadyFlag = true;

        } catch (Exception ex) {
            System.err.printf("Error: %s%n%n", ex.getMessage());
            ex.printStackTrace();
        }//end try

    }//end loadData

    /**
     * get all data sets as list
     *
     * @return List< List<Map<String,String> > > list of the data sets
     * containing the data
     */
    public List<List<Map<String, String>>> getListDataSets() {
        if (!this.dataEngineReadyFlag) {
            throw new RuntimeException("Data engine not initialized with imported data from external files!");
        }//end if 
        return this.dataSetList;
    }//end getListDataSet

    /**
     * get data set by name
     *
     * @return List <Map<String,String>> of data set list of map/records
     * containing the data
     */
    public List<Map<String, String>> getDataSetByName(final String name) {
        if (!this.dataEngineReadyFlag) {
            throw new RuntimeException("Data engine not initialized with imported data from external files!");
        }//end if

        if (this.hasDataSetName(name)) {
            return this.dataSetMap.get(name);
        } else {
            throw new RuntimeException(String.format("DateEngine.getDataSetByName: '%s' is not a valid name for data sets!", name));
        }//end if

    }//end getDataSetByName

    /**
     * check if has data set by name
     *
     * @return boolean if data set by given name exists in data engine
     */
    public boolean hasDataSetName(final String name) {
        if (!this.dataEngineReadyFlag) {
            throw new RuntimeException("Data engine not initialized with imported data from external files!");
        }//end if

        return this.dataSetNames.contains(name);
    }//end hasDataSetName

    /**
     * get names of data sets
     *
     * @return Set<String> set of String of data set names
     */
    public Set<String> getDataSetNames() {
        if (!this.dataEngineReadyFlag) {
            throw new RuntimeException("Data engine not initialized with imported data from external files!");
        }//end if

        return this.dataSetNames;
    }//end getDataSetNames

    /**
     * Returns list of maps/records from reading data from external data file in
     * CSV (comma separated value) format.
     *
     * @param filePath the path to the external CSV data file
     * @return list of map/record read from the external file
     */
    public static List<Map<String, String>> readCSV(final String filePath) {

        if (Files.notExists(Paths.get(filePath))) {
            System.err.printf("Error: File %s not found!%n%n", filePath);
            System.exit(1);
        }//end if

        List<Map<String, String>> list = null;

        try {

            Reader reader = Files.newBufferedReader(Paths.get(filePath));
            CsvParserSettings settings = new CsvParserSettings();

            settings.setQuoteDetectionEnabled(true);

            CsvParser parser = new CsvParser(settings);

            List<String[]> allRows = parser.parseAll(reader);

            list = new ArrayList<Map<String, String>>(allRows.size() - 1);

            String[] headers = allRows.get(0);

            for (int x = 1; x < allRows.size(); x++) {

                String[] row = allRows.get(x);

                Map<String, String> map = new HashMap<>(row.length);
                for (int y = 0; y < row.length; y++) {

                    map.put(headers[y], row[y]);

                } // end for
                list.add(map);
            }//end for

            reader.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }//end try

        return list;

    }// end readCSV

    /**
     * Returns list of maps/records from reading data from external data file in
     * Excel (Microsoft Excel spreadsheet binary) format.
     *
     * @param filePath the path to the external Excel data file
     * @return list of map/record read from the external file
     */
    public static List<Map<String, String>> readExcel(final String filePath) {

        if (Files.notExists(Paths.get(filePath))) {
            System.err.printf("Error: File %s not found!%n%n", filePath);
            System.exit(1);
        }//end if

        List<Map<String, String>> list = new ArrayList<Map<String, String>>();

        try {

            File excelFile = new File(filePath);
            FileInputStream fis = new FileInputStream(excelFile);

            // we create an XSSF Workbook object for our XLSX Excel File
            XSSFWorkbook workbook = new XSSFWorkbook(fis);
            // we get first sheet
            XSSFSheet sheet = workbook.getSheetAt(0);

            //read first row - column headers	
            Iterator<Row> rowIter = sheet.iterator();
            Row row = rowIter.next();

            // iterate on cells for the current row
            Iterator<Cell> cellIter = row.cellIterator();

            List<String> headers = new ArrayList<>();

            while (cellIter.hasNext()) {
                Cell cell = cellIter.next();
                String header = cell.toString().replace('\n', ' '); //change newline \n to space
                headers.add(header);
            }//end while

            final int rowSize = headers.size();

            int idx = 0;

            loopRow:
            while (rowIter.hasNext()) {
                idx = 0;
                Map<String, String> map = new HashMap<>(rowSize);
                row = rowIter.next();

                // iterate on cells for the current row
                cellIter = row.cellIterator();

                while (cellIter.hasNext()) {

                    Cell cell = cellIter.next();
                    if (cell.toString().contentEquals("")) {
                        break loopRow;
                    }//end if
                    map.put(headers.get(idx), cell.toString());

                    idx++;

                }//end while

                list.add(map);

            }//end while

            workbook.close();
            fis.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }//end try

        return list;

    }// end readExcel

    /**
     * Returns list of maps/records from reading data from external data file in
     * JSON (Javascript object notation) format.
     *
     * @param filePath the path to the external JSON data file
     * @return list of map/record read from the external file
     */
    public static List<Map<String, String>> readJSON(final String filePath) {

        if (Files.notExists(Paths.get(filePath))) {
            System.err.printf("Error: File %s not found!%n%n", filePath);
            System.exit(1);
        }//end if

        String json = "";
        List<Map<String, String>> mapList = null;
        try {
            json = new String(Files.readAllBytes(Paths.get(filePath)));

            ObjectMapper objectMapper = new ObjectMapper();
            TypeFactory typeFactory = objectMapper.getTypeFactory();
            List<Object> list = objectMapper.readValue(json, typeFactory.constructCollectionType(List.class, Object.class));

            @SuppressWarnings("unchecked")
			List<String> headers = (List<String>) list.get(0);

            mapList = new ArrayList<>(list.size() - 1);

            for (int x = 1; x < list.size(); x++) {

            	@SuppressWarnings("unchecked")
                List<String> row = (List<String>) list.get(x);

                Map<String, String> map = new HashMap<String, String>(headers.size());

                for (int y = 0; y < headers.size(); y++) {
                    map.put(headers.get(y), row.get(y));
                }//end for

                mapList.add(map);
            }//end for

        } catch (Exception e) {
            e.printStackTrace();
        }//end try

        return mapList;

    }//end readJSON

    /**
     * Returns list of maps/records from reading data from external data file in
     * XML (eXtensible Markup Language) format.
     *
     * @param filePath the path to the external XML data file
     * @return list of map/record read from the external file
     */
    public static List<Map<String, String>> readXML(final String filePath) {

        if (Files.notExists(Paths.get(filePath))) {
            System.err.printf("Error: File %s not found!%n%n", filePath);
            System.exit(1);
        }//end if

        List<Map<String, String>> list = new ArrayList<>();

        Path pathInput = Paths.get(filePath);

        Class<?>[] cls = new Class[]{Map.class, HashMap.class};

        XStream xstream = new XStream(); // new XStream(new StaxDriver());

        XStream.setupDefaultSecurity(xstream);
        xstream.allowTypes(cls);
        xstream.alias("record", Map.class);
        xstream.registerConverter(new MapEntryConverter());

        try {
            BufferedReader reader = Files.newBufferedReader(pathInput, StandardCharsets.UTF_8);

            String line = "";

            line = reader.readLine();

            line = reader.readLine();

            StringBuilder recString = new StringBuilder(800); //initialize with size?

            for (;;) {

                recString.setLength(0); //reset stringbuilder for next record

                line = reader.readLine();
                line = line.trim();
                if (line.contentEquals("</state-county-wage-data>")) {
                    break;  //xml file footer
                }//end if

                if (line.contentEquals("<record>")) {

                    recString.append(line);

                    //read all 19-attributes
                    for (int x = 0; x < 19; x++) {
                        line = reader.readLine();
                        line = line.trim();
                        recString.append(line);

                    }//end for		

                    line = reader.readLine();
                    line = line.trim();
                    if (line.contentEquals("</record>")) {
                        recString.append(line);
                    } else {
                        System.out.printf("Error: Line is not </record> != '%s'%n", line);
                        break;
                    }//end if

                    String recordXML = recString.toString().replaceAll("\\s+", "");

                    @SuppressWarnings("unchecked")
                    Map<String, String> map = (Map<String, String>) xstream.fromXML(recordXML);

                    list.add(map);

                } else {
                    System.out.printf("Error: Line is not <record> != '%s'%n", line);
                    break;
                }//end if

                reader.readLine();

            }//end for

            reader.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        } // end try

        return list;

    }//end readXML 

}//end class DataEngine

