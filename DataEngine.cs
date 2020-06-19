using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace org.nitw.project
{
    class DataEngine
    {
        //files with external datum to import into engine
        public const String fileNameCountyEmploymentWages = "US_St_Cn_Table_Workforce_Wages.xml"; 
        public const String fileNameCountyList            = "usa_county_list.csv";
        public const String fileNameCountyMedianIncome    = "Median_Income_County.json";
        public const String fileNameCountyPopulationTax   = "Population_By_County_State_County_Tax.csv";
        public const String fileNameCountyUnemployment    = "Unemployment_By_County.xlsx";
        //public const String fileNameCountyWorkforceWages  = "Workforce_Wages_By_County_Area_County_NoEmpty.xlsx"; //6
        public const String fileNameStateExports          = "Exports By State 2012.xlsx";
        public const String fileNameStateTaxRates         = "StateTaxRates.xlsx";

        private bool echoImportFlag = true; //echo loading, record size, time
        private bool dataEngineReadyFlag = false; //indicate data engine is ready from data import

        //data sets for each file 
        private List<Dictionary<String, String>> dataSetCountyEmploymentWages = null;
        private List<Dictionary<String, String>> dataSetCountyList = null;
        private List<Dictionary<String, String>> dataSetCountyMedianIncome = null;
        private List<Dictionary<String, String>> dataSetCountyPopulationTax = null;
        private List<Dictionary<String, String>> dataSetCountyUnemployment = null;
        //private List<Dictionary<String, String>> dataSetCountyWorkforceWages = null;
        private List<Dictionary<String, String>> dataSetStateExports = null;
        private List<Dictionary<String, String>> dataSetStateTaxRates = null;

        //data sets stored in list and map for collective access to all data sets
        private List<List<Dictionary<String, String>>> dataSetList = null;
        private Dictionary<String, List<Dictionary<String, String>>> dataSetMap = null;

        //data set names
        private List<String> dataSetNames = null;

        public DataEngine()
        {
        } //end constructor

        public DataEngine(bool echoImportFlag)
        {
            this.echoImportFlag = echoImportFlag;
        } //end constructor

        public void dumpDataSets(int limit) {

            if (!this.dataEngineReadyFlag) {
                throw new Exception("Data engine not initialized with imported data from external files!");
            }//end if

            List<String> dataSets = new List<String>(this.getDataSetNames());
            dataSets.Sort();
            
            foreach(String dataSet in dataSets) {

                List<Dictionary<String, String>> list = this.dataSetMap[dataSet];

                Console.WriteLine("Data Set: {0}", dataSet);

                for (int idx = 0; idx < limit; idx++) {

                    Dictionary<String, String> map = (Dictionary<String, String>) list[idx];

                    List<String> keySet = new List<String>(map.Keys);

                    foreach(String key in keySet) {

                        String val = map[key];

                        Console.WriteLine("  key:'{0}' => val:'{1}' ", key, val);

                    }//end for
                    Console.WriteLine();
                    
                }//end for

                Console.WriteLine();

            }//end for

        }//end dumpDataSets

        public List<List<Dictionary<String, String>>> getListDataSets()
        {
            if (!this.dataEngineReadyFlag)
            {
                throw new Exception("Data engine not initialized with imported data from external files!");
            } //end if 

            return this.dataSetList;
        } //end getListDataSet

        public bool hasDataSetName(String name)
        {
            if (!this.dataEngineReadyFlag)
            {
                throw new Exception("Data engine not initialized with imported data from external files!");
            } //end if

            return this.dataSetNames.Contains(name);
        } //end hasDataSetName

        public List<Dictionary<String, String>> getDataSetByName(String name)
        {
            if (!this.dataEngineReadyFlag)
            {
                throw new Exception("Data engine not initialized with imported data from external files!");
            } //end if

            if (this.hasDataSetName(name))
            {
                return this.dataSetMap[name];
            }
            else
            {
                throw new Exception("DateEngine.getDataSetByName: " + name + "is not a valid name for data sets!");
            } //end if
        } //end getDataSetByName

        public List<String> getDataSetNames()
        {
            if (!this.dataEngineReadyFlag)
            {
                throw new Exception("Data engine not initialized with imported data from external files!");
            } //end if

            return this.dataSetNames;
        } //end getDataSetNames

        public static List<Dictionary<String, String>> importData(String filePath)
        {
            //check if file exists, if not exit 1
            if (!File.Exists(filePath))
            {
                Console.Error.WriteLine();
                Console.Error.WriteLine();
                Console.Error.WriteLine("Error: File {0} not found!", filePath);
                Console.Error.WriteLine();
                System.Environment.Exit(1);
            }//end if

            List<Dictionary<String, String>> list = null;

            String[] filePathExt = filePath.Split("\\.");

            switch (filePathExt[1])
            {
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
                    Console.WriteLine("Error: Unknown file extension: " + filePathExt[1] + "!");
                    System.Environment.Exit(1);
                    break;
            } //end switch

            return list;
        } //end importData

        public void loadData()
        {
            long totalTime = 0L, totalSize = 0L;

            long timeStart = 0L, timeClose = 0L;

            Stopwatch.StartNew();
            if (this.echoImportFlag)
            {
                Console.WriteLine("Starting Import Data Sets from Files.");
                Console.WriteLine();
            } //end if

            try
            {
                if (this.echoImportFlag)
                {
                    Console.Write("  Import County Population Tax CSV...    ");
                } //end if

                timeStart = Stopwatch.GetTimestamp();


                this.dataSetCountyPopulationTax = DataEngine.importData(DataEngine.fileNameCountyPopulationTax);
                timeClose = Stopwatch.GetTimestamp();

                if (this.echoImportFlag)
                {
                    Console.WriteLine("Done. {0}-records loaded. Time: {1}-mSec.%n%n",
                        this.dataSetCountyPopulationTax.Count, (timeClose - timeStart));
                } //end if

                totalTime = totalTime + (timeClose - timeStart);
                totalSize = totalSize + this.dataSetCountyPopulationTax.Count;

                if (this.echoImportFlag)
                {
                    Console.Write("  Import USA County List CSV...          ");
                } //end if

                timeStart = Stopwatch.GetTimestamp();

                this.dataSetCountyList = DataEngine.importData(DataEngine.fileNameCountyList);

                timeClose = Stopwatch.GetTimestamp();

                if (this.echoImportFlag)
                {
                    Console.WriteLine("Done. {0}-records loaded. Time: {1}d-mSec.", this.dataSetCountyList.Count,
                        (timeClose - timeStart));
                } //end if

                totalTime = totalTime + (timeClose - timeStart);
                totalSize = totalSize + this.dataSetCountyList.Count;

                if (this.echoImportFlag)
                {
                    Console.Write("  Import County Unemployment Excel...    ");
                } //end if

                timeStart = Stopwatch.GetTimestamp();
                this.dataSetCountyUnemployment = DataEngine.importData(DataEngine.fileNameCountyUnemployment);
                timeClose = Stopwatch.GetTimestamp();

                if (this.echoImportFlag)
                {
                    Console.WriteLine("Done. {0}-records loaded. Time: {1}-mSec.",
                        this.dataSetCountyPopulationTax.Count, (timeClose - timeStart));
                    Console.WriteLine();
                } //end if

                totalTime = totalTime + (timeClose - timeStart);
                totalSize = totalSize + this.dataSetCountyUnemployment.Count;

                //*
                if (this.echoImportFlag)
                {
                    Console.Write("  Import County Workforce Wages Excel... ");
                } //end if

/*
                timeStart = Stopwatch.GetTimestamp();
                this.dataSetCountyWorkforceWages = DataEngine.importData(DataEngine.fileNameCountyWorkforceWages);
                timeClose = Stopwatch.GetTimestamp();

                if (this.echoImportFlag)
                {
                    Console.WriteLine("Done. {0}-records loaded. Time: {1}-mSec.",
                        this.dataSetCountyWorkforceWages.Count, (timeClose - timeStart));
                    Console.WriteLine();
                } //end if

                totalTime = totalTime + (timeClose - timeStart);
                totalSize = totalSize + this.dataSetCountyWorkforceWages.Count;
//*/

                if (this.echoImportFlag)
                {
                    Console.Write("  Import Exports By State Excel...       ");
                } //end if

                timeStart = Stopwatch.GetTimestamp();
                this.dataSetStateExports = DataEngine.importData(DataEngine.fileNameStateExports);
                timeClose = Stopwatch.GetTimestamp();

                if (this.echoImportFlag)
                {
                    Console.WriteLine("Done. {0}-records loaded. Time: {1}-mSec.%n%n", this.dataSetStateExports.Count,
                        (timeClose - timeStart));
                    Console.WriteLine();
                } //end if

                totalTime = totalTime + (timeClose - timeStart);
                totalSize = totalSize + this.dataSetStateExports.Count;

                if (this.echoImportFlag)
                {
                    Console.Write("  Import State Tax Rates Excel...        ");
                } //end if

                timeStart = Stopwatch.GetTimestamp();
                this.dataSetStateTaxRates = DataEngine.importData(DataEngine.fileNameStateTaxRates);
                timeClose = Stopwatch.GetTimestamp();

                if (this.echoImportFlag)
                {
                    Console.WriteLine("Done. {0}-records loaded. Time: {1}-mSec.", this.dataSetStateTaxRates.Count,
                        (timeClose - timeStart));
                    Console.WriteLine();
                } //end if

                totalTime = totalTime + (timeClose - timeStart);
                totalSize = totalSize + this.dataSetStateTaxRates.Count;

                if (this.echoImportFlag)
                {
                    Console.Write("  Import County Median Income JSON...    ");
                } //end if

                timeStart = Stopwatch.GetTimestamp();
                this.dataSetCountyMedianIncome = DataEngine.importData(DataEngine.fileNameCountyMedianIncome);
                timeClose = Stopwatch.GetTimestamp();

                if (this.echoImportFlag)
                {
                    Console.WriteLine("Done. {0}-records loaded. Time: {1}-mSec.%n%n",
                        this.dataSetCountyMedianIncome.Count, (timeClose - timeStart));
                    Console.WriteLine();
                } //end if

                totalTime = totalTime + (timeClose - timeStart);
                totalSize = totalSize + this.dataSetCountyMedianIncome.Count;

                if (this.echoImportFlag)
                {
                    Console.Write("  Import County Employment Wages XML...  ");
                } //end if

                timeStart = Stopwatch.GetTimestamp();
                this.dataSetCountyEmploymentWages = DataEngine.importData(DataEngine.fileNameCountyEmploymentWages);
                timeClose = Stopwatch.GetTimestamp();

                if (this.echoImportFlag)
                {
                    Console.WriteLine("Done. {0}-records loaded. Time: {1}-mSec.%n%n",
                        this.dataSetCountyEmploymentWages.Count, (timeClose - timeStart));
                    Console.WriteLine();
                } //end if

                totalTime = totalTime + (timeClose - timeStart);
                totalSize = totalSize + this.dataSetCountyEmploymentWages.Count;

                if (this.echoImportFlag)
                {
                    Console.WriteLine("Finished Import Data Sets from Files.");
                    Console.WriteLine();
                    Console.WriteLine("Total {0}-records imported in {1}-mSec.", totalSize, totalTime);
                    Console.WriteLine();
                } //end if

                //create the list and map that allow collective/centralized access to all data sets
                this.dataSetList = new List<List<Dictionary<string, string>>>(8);
                this.dataSetMap = new Dictionary<string, List<Dictionary<string, string>>>(8);
                
                this.dataSetList.Add(this.dataSetCountyEmploymentWages);
                this.dataSetMap["CountyEmploymentWages"] = this.dataSetCountyEmploymentWages;

                this.dataSetList.Add(this.dataSetCountyList);
                this.dataSetMap["CountyList"] = this.dataSetCountyList;

                this.dataSetList.Add(this.dataSetCountyMedianIncome);
                this.dataSetMap["CountyMedianIncome"] = this.dataSetCountyMedianIncome;

                this.dataSetList.Add(this.dataSetCountyPopulationTax);
                this.dataSetMap["CountyPopulationTax"] = this.dataSetCountyPopulationTax;

                this.dataSetList.Add(this.dataSetCountyUnemployment);
                this.dataSetMap["CountyUnemployment"] = this.dataSetCountyUnemployment;

                //this.dataSetList.Add(this.dataSetCountyWorkforceWages);
                //this.dataSetMap["CountyWorkforceWages"] = this.dataSetCountyWorkforceWages;

                this.dataSetList.Add(this.dataSetStateExports);
                this.dataSetMap["StateExports"] = this.dataSetStateExports;

                this.dataSetList.Add(this.dataSetStateTaxRates);
                this.dataSetMap["StateTaxRates"] = this.dataSetStateTaxRates;

                //initialize list of each data set name using the map
                this.dataSetNames = new List<string>(this.dataSetMap.Keys);

                this.dataEngineReadyFlag = true;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Error: " + ex.Message + "!");
                Console.Error.WriteLine();
            } //end try

        } //end loadData

        //stub methods for readXXX file type
        private static List<Dictionary<String, String>> readCSV(String filePath)
        {
            List<Dictionary<String, String>> list = null;

            return list;
            
        } //end readCSV

        private static List<Dictionary<String, String>> readExcel(String filePath)
        {
            List<Dictionary<String, String>> list = null;

            return list;
            
        } //end readExcel

        private static List<Dictionary<String, String>> readJSON(String filePath)
        {
            List<Dictionary<String, String>> list = null;

            return list;
            
        } //end readJSON

        private static List<Dictionary<String, String>> readXML(String filePath)
        {
            List<Dictionary<String, String>> list = null;

            return list;
            
        } //end readXML
        
        public static void Main(string[] args)
        {
            DataEngine dEng = new DataEngine();

            //load file data into engine data sets; then dump first 4-records of each
            //and list the data sets by name in descending sorted order. Then access
            //data set by name to get list of records/maps.

            try {

                //load or import data from external files
                dEng.loadData();

                //dump first four records of each data set
                dEng.dumpDataSets(4);

                //list the names of data sets in sorted descending order
                Console.WriteLine("Listing Data Set by Name:");
                Console.WriteLine();

                List<String> list = new List<String>(dEng.getDataSetNames());
                list.Sort();

                foreach (String dataSetName in list) {
                    Console.WriteLine("  {0}", dataSetName);
                }//end for

                Console.WriteLine();
                Console.WriteLine();

                //get data set by name directly 
                List< Dictionary<String, String>> dataSet = dEng.getDataSetByName("CountyUnemployment");

            } catch (Exception ex) {
                Console.Error.WriteLine();
                Console.Error.WriteLine("Error: {0}", ex.Message);
                Console.Error.WriteLine();
                Console.Error.WriteLine(ex.StackTrace);
            }//end try
            
            System.Environment.Exit(0);
            
        } //end main
        
    } //end class DataEngine
    
} //end namespace org.nitw.project