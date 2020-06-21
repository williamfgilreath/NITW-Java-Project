/**
 *
 * Title:        DataEngine.java - Data engine for project.
 *
 * Description:  Create DataEngine to import, and allow access, query various
 * data values for project for 8-different data sets managed by
 * the data engine.
 *
 * The data sets are stored in external files of different types
 * of formats (CSV, Excel, JSON, XML) of data from free public
 * data from U.S. Census Bureau, Bureau of Labor Statistics, and
 * Bureau of Economic Analysis.
 *
 * Copyright:    Copyright © (c) 2020 Neurodiversity In The Workplace (NITW)
 *
 * Development:  Developed and written by the contributions from Sean Gill,
 * Joseph Riddle, and Christine P. Chai, Ph.D.
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
 * along with this program.  If not, see <https:></https:>//www.gnu.org/licenses/>.
 *
 */
package org.nitw.project

import com.fasterxml.jackson.databind.ObjectMapper
import com.thoughtworks.xstream.XStream
import com.thoughtworks.xstream.converters.Converter
import com.thoughtworks.xstream.converters.MarshallingContext
import com.thoughtworks.xstream.converters.UnmarshallingContext
import com.thoughtworks.xstream.io.HierarchicalStreamReader
import com.thoughtworks.xstream.io.HierarchicalStreamWriter
import com.univocity.parsers.csv.CsvParser
import com.univocity.parsers.csv.CsvParserSettings
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.File
import java.io.FileInputStream
import java.io.Reader
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

//Apache POI - the Java API for Microsoft Documents https://poi.apache.org
//Jackson is a suite of data-processing tools for Java https://github.com/FasterXML/jackson
//XStream is a simple library to serialize objects to XML and back again. https://x-stream.github.io
//univocity-parsers is a collection of extremely fast and reliable parsers for Java. https://github.com/uniVocity/univocity-parsers
//standard Java input-output 
//standard Java new input-output
//standard Java data structures/collections
class DataEngine {
    //auxiliary class used by XStream library to process XML into Java map data structure
    internal class MapEntryConverter : Converter {
        override fun canConvert(cls: Class<*>?): Boolean {
            return AbstractMap::class.java.isAssignableFrom(cls)
        } //end canConvert

        override fun marshal(value: Any, writer: HierarchicalStreamWriter, context: MarshallingContext) {
            val map = value as AbstractMap<*, *>
            for (obj in map.entries) {
                val entry = obj as Map.Entry<*, *>
                writer.startNode(entry.key.toString())
                val `val` = entry.value
                if (null != `val`) {
                    writer.setValue(`val`.toString())
                }
                writer.endNode()
            } //end for
        } //end marshal

        override fun unmarshal(reader: HierarchicalStreamReader, context: UnmarshallingContext): Any {
            val map: MutableMap<String, String> = HashMap()
            while (reader.hasMoreChildren()) {
                reader.moveDown()
                val key = reader.nodeName // nodeName aka element's name
                val value = reader.value
                map[key] = value
                reader.moveUp()
            } //end while
            return map
        } //end unmarshal
    } //end class MapEntryConverter

    private var echoImportFlag = true //echo loading, record size, time
    private var dataEngineReadyFlag = false //indicate data engine is ready from data import

    /**
     * null constructor uses default settings
     */
    constructor() {} //end constructor

    /**
     * constructor to take Boolean echoImportFlag
     *
     * @param echoImportFlag indicates to echo each data import from external
     * file of time, number of records.
     */
    constructor(echoImportFlag: Boolean) {
        this.echoImportFlag = echoImportFlag
    } //end constructor

    //data sets for each file 
    private var dataSetCountyEmploymentWages: List<Map<String, String>>? = null
    private var dataSetCountyList: List<Map<String, String>>? = null
    private var dataSetCountyMedianIncome: List<Map<String, String>>? = null
    private var dataSetCountyPopulationTax: List<Map<String, String>>? = null
    private var dataSetCountyUnemployment: List<Map<String, String>>? = null
    private var dataSetStateExports: List<Map<String, String>>? = null
    private var dataSetStateTaxRates: List<Map<String, String>>? = null

    //data sets stored in list and map for collective access to all data sets
    private var dataSetList: MutableList<List<Map<String, String>>?>? = null
    private var dataSetMap: MutableMap<String, List<Map<String, String>>?>? = null

    //data set names
    private var dataSetNames: Set<String>? = null

    /**
     * Method that dumps map/record in each data set to a limit >= 1
     *
     * @param limit - how many records to dump as String from each data set
     */
    fun dumpDataSets(limit: Int) {
        if (!dataEngineReadyFlag) {
            throw RuntimeException("Data engine not initialized with imported data from external files!")
        } //end if
        val dataSets: List<String> = ArrayList(getDataSetNames())
        Collections.sort(dataSets)
        for (dataSet in dataSets) {
            val list = dataSetMap!![dataSet]
            System.out.printf("Data Set: %s%n", dataSet)
            for (idx in 0 until limit) {
                val map = list!![idx]

                //System.out.printf("%s%n", map.toString().replace("{", "").replace("}", ""));
                val keySet = map.keys
                for (key in keySet) {
                    val `val` = map[key]
                    System.out.printf("  key:'%s' => val:'%s' ", key, `val`)
                } //end for
                println()
            } //end for
            println()
        } //end for
    } //end dumpDataSets

    /**
     * Import or load datum from external data files into internal data
     * structures
     *
     */
    fun loadData() {
        var totalTime = 0L
        var totalSize = 0L
        var timeStart = 0L
        var timeClose = 0L
        if (echoImportFlag) {
            System.out.printf("Starting Import Data Sets from Files.%n%n")
        } //end if
        try {
            if (echoImportFlag) {
                print("  Import County Population Tax CSV...    ")
            } //end if
            timeStart = System.currentTimeMillis()
            dataSetCountyPopulationTax = importData(fileNameCountyPopulationTax)
            timeClose = System.currentTimeMillis()
            if (echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", dataSetCountyPopulationTax!!.size, timeClose - timeStart)
            } //end if
            totalTime = totalTime + (timeClose - timeStart)
            totalSize = totalSize + dataSetCountyPopulationTax!!.size
            if (echoImportFlag) {
                print("  Import USA County List CSV...          ")
            } //end if
            timeStart = System.currentTimeMillis()
            dataSetCountyList = importData(fileNameCountyList)
            timeClose = System.currentTimeMillis()
            if (echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", dataSetCountyList!!.size, timeClose - timeStart)
            } //end if
            totalTime = totalTime + (timeClose - timeStart)
            totalSize = totalSize + dataSetCountyList!!.size
            if (echoImportFlag) {
                print("  Import County Unemployment Excel...    ")
            } //end if
            timeStart = System.currentTimeMillis()
            dataSetCountyUnemployment = importData(fileNameCountyUnemployment)
            timeClose = System.currentTimeMillis()
            if (echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", dataSetCountyPopulationTax!!.size, timeClose - timeStart)
            } //end if
            totalTime = totalTime + (timeClose - timeStart)
            totalSize = totalSize + dataSetCountyUnemployment!!.size
            if (echoImportFlag) {
                print("  Import Exports By State Excel...       ")
            } //end if
            timeStart = System.currentTimeMillis()
            dataSetStateExports = importData(fileNameStateExports)
            timeClose = System.currentTimeMillis()
            if (echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", dataSetStateExports!!.size, timeClose - timeStart)
            } //end if
            totalTime = totalTime + (timeClose - timeStart)
            totalSize = totalSize + dataSetStateExports!!.size
            if (echoImportFlag) {
                print("  Import State Tax Rates Excel...        ")
            } //end if
            timeStart = System.currentTimeMillis()
            dataSetStateTaxRates = importData(fileNameStateTaxRates)
            timeClose = System.currentTimeMillis()
            if (echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", dataSetStateTaxRates!!.size, timeClose - timeStart)
            } //end if
            totalTime = totalTime + (timeClose - timeStart)
            totalSize = totalSize + dataSetStateTaxRates!!.size
            if (echoImportFlag) {
                print("  Import County Median Income JSON...    ")
            } //end if
            timeStart = System.currentTimeMillis()
            dataSetCountyMedianIncome = importData(fileNameCountyMedianIncome)
            timeClose = System.currentTimeMillis()
            if (echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", dataSetCountyMedianIncome!!.size, timeClose - timeStart)
            } //end if
            totalTime = totalTime + (timeClose - timeStart)
            totalSize = totalSize + dataSetCountyMedianIncome!!.size
            if (echoImportFlag) {
                print("  Import County Employment Wages XML...  ")
            } //end if
            timeStart = System.currentTimeMillis()
            dataSetCountyEmploymentWages = importData(fileNameCountyEmploymentWages)
            timeClose = System.currentTimeMillis()
            if (echoImportFlag) {
                System.out.printf("Done. %6d-records loaded. Time: %6d-mSec.%n%n", dataSetCountyEmploymentWages!!.size, timeClose - timeStart)
            } //end if
            totalTime = totalTime + (timeClose - timeStart)
            totalSize = totalSize + dataSetCountyEmploymentWages!!.size
            if (echoImportFlag) {
                System.out.printf("Finished Import Data Sets from Files.%n%n")
                System.out.printf("Total %d-records imported in %d-mSec.%n%n", totalSize, totalTime)
            } //end if

            //create the list and map that allow collective/centralized access to all data sets
            dataSetList = ArrayList(8)
            dataSetMap = HashMap(8)
            dataSetList.add(dataSetCountyEmploymentWages)
            dataSetMap["CountyEmploymentWages"] = dataSetCountyEmploymentWages
            dataSetList.add(dataSetCountyList)
            dataSetMap["CountyList"] = dataSetCountyList
            dataSetList.add(dataSetCountyMedianIncome)
            dataSetMap["CountyMedianIncome"] = dataSetCountyMedianIncome
            dataSetList.add(dataSetCountyPopulationTax)
            dataSetMap["CountyPopulationTax"] = dataSetCountyPopulationTax
            dataSetList.add(dataSetCountyUnemployment)
            dataSetMap["CountyUnemployment"] = dataSetCountyUnemployment

//            this.dataSetList.add(this.dataSetCountyWorkforceWages);
//            this.dataSetMap.put("CountyWorkforceWages", this.dataSetCountyWorkforceWages);
            dataSetList.add(dataSetStateExports)
            dataSetMap["StateExports"] = dataSetStateExports
            dataSetList.add(dataSetStateTaxRates)
            dataSetMap["StateTaxRates"] = dataSetStateTaxRates

            //initialize list of each data set name using the map
            dataSetNames = dataSetMap.keys
            dataEngineReadyFlag = true
        } catch (ex: Exception) {
            System.err.printf("Error: %s%n%n", ex.message)
            ex.printStackTrace()
        } //end try
    } //end loadData//end if 
    //end getListDataSet

    /**
     * get all data sets as list
     *
     * @return List< List<Map></Map><String></String>,String> > > list of the data sets
     * containing the data
     */
    val listDataSets: List<List<Map<String, String>>?>?
        get() {
            if (!dataEngineReadyFlag) {
                throw RuntimeException("Data engine not initialized with imported data from external files!")
            } //end if 
            return dataSetList
        }

    /**
     * get data set by name
     *
     * @return List <Map></Map><String></String>,String>> of data set list of map/records
     * containing the data
     */
    fun getDataSetByName(name: String): List<Map<String, String>>? {
        if (!dataEngineReadyFlag) {
            throw RuntimeException("Data engine not initialized with imported data from external files!")
        } //end if
        return if (hasDataSetName(name)) {
            dataSetMap!![name]
        } else {
            throw RuntimeException(String.format("DateEngine.getDataSetByName: '%s' is not a valid name for data sets!", name))
        } //end if
    } //end getDataSetByName

    /**
     * check if has data set by name
     *
     * @return boolean if data set by given name exists in data engine
     */
    fun hasDataSetName(name: String): Boolean {
        if (!dataEngineReadyFlag) {
            throw RuntimeException("Data engine not initialized with imported data from external files!")
        } //end if
        return dataSetNames!!.contains(name)
    } //end hasDataSetName

    /**
     * get names of data sets
     *
     * @return Set<String> set of String of data set names
    </String> */
    fun getDataSetNames(): Set<String>? {
        if (!dataEngineReadyFlag) {
            throw RuntimeException("Data engine not initialized with imported data from external files!")
        } //end if
        return dataSetNames
    } //end getDataSetNames

    companion object {
        //files with external datum to import into engine
        const val fileNameCountyEmploymentWages = "US_St_Cn_Table_Workforce_Wages.xml" //1
        const val fileNameCountyList = "usa_county_list.csv"
        const val fileNameCountyMedianIncome = "Median_Income_County.json"
        const val fileNameCountyPopulationTax = "Population_By_County_State_County_Tax.csv"
        const val fileNameCountyUnemployment = "Unemployment_By_County.xlsx"
        const val fileNameStateExports = "Exports By State 2012.xlsx"
        const val fileNameStateTaxRates = "StateTaxRates.xlsx"
        fun importData(filePath: String): List<Map<String, String>>? {
            if (Files.notExists(Paths.get(filePath))) {
                System.err.printf("Error: File %s not found!%n%n", filePath)
                System.exit(1)
            } //end if
            var list: List<Map<String, String>>? = null
            val filePathExt = filePath.split("\\.".toRegex()).toTypedArray()
            when (filePathExt[1]) {
                "csv" -> list = readCSV(filePath)
                "json" -> list = readJSON(filePath)
                "xlsx" -> list = readExcel(filePath)
                "xml" -> list = readXML(filePath)
                else -> {
                    System.err.printf("Error: Unknown file extension: %s%n!", filePathExt[1])
                    System.exit(1)
                }
            }
            return list
        } //end importData

        /**
         * Returns list of maps/records from reading data from external data file in
         * CSV (comma separated value) format.
         *
         * @param filePath the path to the external CSV data file
         * @return list of map/record read from the external file
         */
        fun readCSV(filePath: String?): List<Map<String, String>>? {
            if (Files.notExists(Paths.get(filePath))) {
                System.err.printf("Error: File %s not found!%n%n", filePath)
                System.exit(1)
            } //end if
            var list: MutableList<Map<String, String>>? = null
            try {
                val reader: Reader = Files.newBufferedReader(Paths.get(filePath))
                val settings = CsvParserSettings()
                settings.isQuoteDetectionEnabled = true
                val parser = CsvParser(settings)
                val allRows = parser.parseAll(reader)
                list = ArrayList(allRows.size - 1)
                val headers = allRows[0]
                for (x in 1 until allRows.size) {
                    val row = allRows[x]
                    val map: MutableMap<String, String> = HashMap(row.size)
                    for (y in row.indices) {
                        map[headers[y]] = row[y]
                    } // end for
                    list.add(map)
                } //end for
                reader.close()
            } catch (ex: Exception) {
                ex.printStackTrace()
            } //end try
            return list
        } // end readCSV

        /**
         * Returns list of maps/records from reading data from external data file in
         * Excel (Microsoft Excel spreadsheet binary) format.
         *
         * @param filePath the path to the external Excel data file
         * @return list of map/record read from the external file
         */
        fun readExcel(filePath: String?): List<Map<String, String>> {
            if (Files.notExists(Paths.get(filePath))) {
                System.err.printf("Error: File %s not found!%n%n", filePath)
                System.exit(1)
            } //end if
            val list: MutableList<Map<String, String>> = ArrayList()
            try {
                val excelFile = File(filePath)
                val fis = FileInputStream(excelFile)

                // we create an XSSF Workbook object for our XLSX Excel File
                val workbook = XSSFWorkbook(fis)
                // we get first sheet
                val sheet = workbook.getSheetAt(0)

                //read first row - column headers	
                val rowIter: Iterator<Row> = sheet.iterator()
                var row = rowIter.next()

                // iterate on cells for the current row
                var cellIter = row.cellIterator()
                val headers: MutableList<String> = ArrayList()
                while (cellIter.hasNext()) {
                    val cell = cellIter.next()
                    val header = cell.toString().replace('\n', ' ') //change newline \n to space
                    headers.add(header)
                } //end while
                val rowSize = headers.size
                var idx = 0
                loopRow@ while (rowIter.hasNext()) {
                    idx = 0
                    val map: MutableMap<String, String> = HashMap(rowSize)
                    row = rowIter.next()

                    // iterate on cells for the current row
                    cellIter = row.cellIterator()
                    while (cellIter.hasNext()) {
                        val cell = cellIter.next()
                        if (cell.toString().contentEquals("")) {
                            break@loopRow
                        } //end if
                        map[headers[idx]] = cell.toString()
                        idx++
                    } //end while
                    list.add(map)
                } //end while
                workbook.close()
                fis.close()
            } catch (ex: Exception) {
                ex.printStackTrace()
            } //end try
            return list
        } // end readExcel

        /**
         * Returns list of maps/records from reading data from external data file in
         * JSON (Javascript object notation) format.
         *
         * @param filePath the path to the external JSON data file
         * @return list of map/record read from the external file
         */
        fun readJSON(filePath: String?): List<Map<String, String>>? {
            if (Files.notExists(Paths.get(filePath))) {
                System.err.printf("Error: File %s not found!%n%n", filePath)
                System.exit(1)
            } //end if
            var json = ""
            var mapList: MutableList<Map<String, String>>? = null
            try {
                json = String(Files.readAllBytes(Paths.get(filePath)))
                val objectMapper = ObjectMapper()
                val typeFactory = objectMapper.typeFactory
                val list = objectMapper.readValue<List<Any>>(json, typeFactory.constructCollectionType(MutableList::class.java, Any::class.java))
                val headers = list[0] as List<String>
                mapList = ArrayList(list.size - 1)
                for (x in 1 until list.size) {
                    val row = list[x] as List<String>
                    val map: MutableMap<String, String> = HashMap(headers.size)
                    for (y in headers.indices) {
                        map[headers[y]] = row[y]
                    } //end for
                    mapList.add(map)
                } //end for
            } catch (e: Exception) {
                e.printStackTrace()
            } //end try
            return mapList
        } //end readJSON

        /**
         * Returns list of maps/records from reading data from external data file in
         * XML (eXtensible Markup Language) format.
         *
         * @param filePath the path to the external XML data file
         * @return list of map/record read from the external file
         */
        fun readXML(filePath: String?): List<Map<String, String>> {
            if (Files.notExists(Paths.get(filePath))) {
                System.err.printf("Error: File %s not found!%n%n", filePath)
                System.exit(1)
            } //end if
            val list: MutableList<Map<String, String>> = ArrayList()
            val pathInput = Paths.get(filePath)
            val cls = arrayOf<Class<*>>(MutableMap::class.java, HashMap::class.java)
            val xstream = XStream() // new XStream(new StaxDriver());
            XStream.setupDefaultSecurity(xstream)
            xstream.allowTypes(cls)
            xstream.alias("record", MutableMap::class.java)
            xstream.registerConverter(MapEntryConverter())
            try {
                val reader = Files.newBufferedReader(pathInput, StandardCharsets.UTF_8)
                var line = ""
                line = reader.readLine()
                line = reader.readLine()
                val recString = StringBuilder(800) //initialize with size?
                while (true) {
                    recString.setLength(0) //reset stringbuilder for next record
                    line = reader.readLine()
                    line = line.trim { it <= ' ' }
                    if (line.contentEquals("</state-county-wage-data>")) {
                        break //xml file footer
                    } //end if
                    if (line.contentEquals("<record>")) {
                        recString.append(line)

                        //read all 19-attributes
                        for (x in 0..18) {
                            line = reader.readLine()
                            line = line.trim { it <= ' ' }
                            recString.append(line)
                        } //end for		
                        line = reader.readLine()
                        line = line.trim { it <= ' ' }
                        if (line.contentEquals("</record>")) {
                            recString.append(line)
                        } else {
                            System.out.printf("Error: Line is not </record> != '%s'%n", line)
                            break
                        } //end if
                        val recordXML = recString.toString().replace("\\s+".toRegex(), "")
                        val map = xstream.fromXML(recordXML) as Map<String, String>
                        list.add(map)
                    } else {
                        System.out.printf("Error: Line is not <record> != '%s'%n", line)
                        break
                    } //end if
                    reader.readLine()
                }
                reader.close()
            } catch (ex: Exception) {
                ex.printStackTrace()
            } // end try
            return list
        } //end readXML 

        /**
         * Main method to create instance of data engine, load data, and report as
         * data is loaded from external data files. ode.
         *
         * @param args unused String array of arguments
         */
        @JvmStatic
        fun main(args: Array<String>) {
            val dEng = DataEngine()

            //load file data into engine data sets; then dump first 4-records of each
            //and list the data sets by name in descending sorted order. Then access
            //data set by name to get list of records/maps.
            try {

                //load or import data from external files
                dEng.loadData()

                //dump first four records of each data set
                dEng.dumpDataSets(4)

                //list the names of data sets in sorted descending order
                System.out.printf("Listing Data Set by Name:%n%n")

                //List<String> list = new ArrayList<String>(dEng.getDataSetNames());
                val list = dEng.getDataSetNames()
                //Collections.sort(list);
                for (dataSetName in list!!) {
                    System.out.printf("  %s%n", dataSetName)
                } //end for
                System.out.printf("%n%n")

                //get data set by name directly
                val dataSet = dEng.getDataSetByName("CountyUnemployment")
            } catch (ex: Exception) {
                System.err.printf("Error: %s%n", ex.message)
                ex.printStackTrace()
            } //end try
            System.exit(0)
        } //end main
    }
} //end class DataEngine
