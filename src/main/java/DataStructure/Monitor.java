package DataStructure;

public class Monitor {
    public long callAddToSkyline = 0; //the number of times of calling the add to skyline function
    public long finnalCallAddToSkyline = 0; // the number of times to try to add new backbone path into the result set in the index version function.

    public long runningtime_supplement_addtoskyline = 0;
    public long runningtime_supplement_construction = 0;
    public long runningtime_combination_construction = 0;
    public long runningtime_combination_addtoskyline = 0;
    public long runningtime_dest_addtoskyline;
    public long runningtime_src_addtoskyline;
    public long runningtime_src_create_newpath;
    public long runningtime_dest_create_newpath;
    public long runningtime_check_domination_result;
    public long node_call_addtoskyline; // The number of times of calling the addToSkyline function on each myNode object, in the baseline function.
    public long callcheckdominatedbyresult; // The number of times of check whether the temporary results are dominated by the results in the final result set.
    public long allsizeofthecheckdominatedbyresult; // the total size of the result is the dominate checking by (final or temp) the backbone path　


    public long overallRuningtime; //overall running time of the index query, t= index_reading_time + query_time.
    public long indexQueryTime; //the time that is used to query by using the backbone index
    public long spInitTimeInBaseline; //the time that is used to find the shortest path at the beginning of the baseline algorithm.

    public long sizeOfResult;
    public int coveredNodes; //number of nodes were accessed during the query process


    public void clone(Monitor m) {
        callAddToSkyline = m.callAddToSkyline;
        finnalCallAddToSkyline = m.finnalCallAddToSkyline;

        runningtime_supplement_addtoskyline = m.runningtime_supplement_addtoskyline;
        runningtime_supplement_construction = m.runningtime_supplement_construction;
        runningtime_combination_construction = m.runningtime_combination_construction;
        runningtime_combination_addtoskyline = m.runningtime_combination_addtoskyline;
        runningtime_dest_addtoskyline = m.runningtime_dest_addtoskyline;
        runningtime_src_addtoskyline = m.runningtime_src_addtoskyline;
        runningtime_src_create_newpath = m.runningtime_src_create_newpath;
        runningtime_dest_create_newpath = m.runningtime_dest_create_newpath;
        runningtime_check_domination_result = m.runningtime_check_domination_result;
        node_call_addtoskyline = m.node_call_addtoskyline;

        callcheckdominatedbyresult = m.callcheckdominatedbyresult;
        allsizeofthecheckdominatedbyresult = m.allsizeofthecheckdominatedbyresult;


        overallRuningtime = m.overallRuningtime;
        indexQueryTime = m.indexQueryTime;
        spInitTimeInBaseline = m.spInitTimeInBaseline;

        this.sizeOfResult = m.sizeOfResult;
    }

    public Monitor() {

    }

    public double getRunningtime_supplement_addtoskylineByms() {
        return runningtime_supplement_addtoskyline / 1000000.0;
    }

    public double getRunningtime_supplement_constructionByms() {
        return runningtime_supplement_construction / 1000000.0;
    }

    public double getRunningtime_combination_constructionByms() {
        return runningtime_combination_construction / 1000000.0;
    }

    public double getRunningtime_combination_addtoskylineByms() {
        return runningtime_combination_addtoskyline / 1000000.0;
    }

    public double getRunningtime_dest_addtoskylineByms() {
        return runningtime_dest_addtoskyline / 1000000.0;
    }

    public double getRunningtime_src_addtoskylineByms() {
        return runningtime_src_addtoskyline / 1000000.0;
    }

    public double getRunningtime_intermedia_addtoskyline() {
        return getRunningtime_src_addtoskylineByms() + getRunningtime_dest_addtoskylineByms();
    }


    public double getRunningtime_dest_createByms() {
        return runningtime_dest_create_newpath / 1000000.0;
    }

    public double getRunningtime_src_createlineByms() {
        return runningtime_src_create_newpath / 1000000.0;
    }

    public double getRunningtime_intermedia_createline() {
        return getRunningtime_dest_createByms() + getRunningtime_src_createlineByms();
    }

    public double getRunningtime_check_domination_resultByms() {
        return runningtime_check_domination_result / 1000000.0;
    }

    public long getSizeOfResult() {
        return sizeOfResult;
    }

    public void setSizeOfResult(long sizeOfResult) {
        this.sizeOfResult = sizeOfResult;
    }

    public double getOverallRuningtime_in_Sec() {
        return overallRuningtime / 1000.0;
    }

    public void clear() {
        callAddToSkyline = 0;
        finnalCallAddToSkyline = 0;

        runningtime_supplement_addtoskyline = 0;
        runningtime_supplement_construction = 0;
        runningtime_combination_construction = 0;
        runningtime_combination_addtoskyline = 0;
        runningtime_dest_addtoskyline = 0;
        runningtime_src_addtoskyline = 0;
        runningtime_src_create_newpath = 0;
        runningtime_dest_create_newpath = 0;
        runningtime_check_domination_result = 0;
        node_call_addtoskyline = 0;

        callcheckdominatedbyresult = 0;
        allsizeofthecheckdominatedbyresult = 0;


        overallRuningtime = 0;
        indexQueryTime = 0;
        spInitTimeInBaseline = 0;

        this.sizeOfResult = 0;
    }
}
