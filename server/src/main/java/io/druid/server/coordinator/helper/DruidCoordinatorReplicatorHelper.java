package io.druid.server.coordinator.helper;

import com.metamx.emitter.EmittingLogger;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;

import java.util.*;


public class DruidCoordinatorReplicatorHelper {

    private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorReplicatorHelper.class);

    // 1.1
    public static HashMap<DruidServerMetadata,Integer> metadataToID(HashMap<DruidServerMetadata, Long> nodeCapacities) {
        HashMap<DruidServerMetadata, Integer> ret  = new HashMap<DruidServerMetadata, Integer>();
        int count = 0;
        for(Map.Entry<DruidServerMetadata, Long> entry : nodeCapacities.entrySet()){
           if(!ret.containsKey(entry.getKey())){
                ret.put(entry.getKey(), count);
                count++;
           }
        }
        return ret;
    }

    // 1.2
    public static HashMap<Integer,DruidServerMetadata> IDToMetadata(HashMap<DruidServerMetadata, Integer> metadataToIDMap) {

        HashMap<Integer,DruidServerMetadata> ret = new HashMap<Integer, DruidServerMetadata>();
        for(Map.Entry<DruidServerMetadata, Integer> entry : metadataToIDMap.entrySet()){
            if(!ret.containsKey(entry.getValue())){
                ret.put(entry.getValue(), entry.getKey());
            }
            else{
                log.info("IDToMetaData: ID [%s] contains multiple server map to it");
            }
        }
        return ret;
    }

    // 2, 3
    public static HashMap< Integer, HashMap<DataSegment, Integer>>  constructReverseMap(
            HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable,
            HashMap<DruidServerMetadata, Integer> metadataToIDMap){

        HashMap<Integer, HashMap<DataSegment, Integer>> ret = new HashMap<Integer, HashMap<DataSegment, Integer>>();
        for(Map.Entry<DataSegment, HashMap<DruidServerMetadata,Long>> entry : routingTable.entrySet()){
            for(Map.Entry<DruidServerMetadata, Long> e : entry.getValue().entrySet()){
                int id = metadataToIDMap.get(e.getKey());
                if(!ret.containsKey(id)){
                    //id doesnt exist
                    HashMap<DataSegment, Integer> map = new HashMap<DataSegment, Integer>();
                    //if id doesnt exist, then the value map doesn exit either, assume nothing in the map
                    map.put(entry.getKey(), 1);
                    ret.put(id, map);
                }
                else{
                    //id exists
                    if(!ret.get(id).containsKey(entry.getKey())){
                        //segment doesnt exists
                        HashMap<DataSegment, Integer> map = new HashMap<DataSegment, Integer>();
                        ret.get(id).put(entry.getKey(), 1);
                    }
                    else{
                        //segment also exists
                        int orig_count = ret.get(id).get(entry.getKey());
                        orig_count++;
                        ret.get(id).put(entry.getKey(), orig_count);
                    }

                }
            }
        }

        return ret;
    }


    // 4
    public static int[][] buildCostMatrix (HashMap< Integer, HashMap<DataSegment, Integer>> before,
                                           HashMap< Integer, HashMap<DataSegment, Integer>> after){

        if(before.size()!=after.size()){
            log.info("buildCostMatrix: before and after map have different sizes");
        }

        printRoutingMaps(before, "BEFORE MAP:");
        printRoutingMaps(after, "AFTER MAP:");
        int[][] ret = new int[before.size()][after.size()];

        for(Map.Entry<Integer, HashMap<DataSegment, Integer>> entryA : before.entrySet()){
            for(Map.Entry<Integer, HashMap<DataSegment,Integer>> entryB : after.entrySet()){
                int row = entryA.getKey();
                int col = entryB.getKey();
                int cost = 0;

                for(Map.Entry<DataSegment, Integer> e : entryB.getValue().entrySet()){
                    if(!entryA.getValue().containsKey(e.getKey())){
                        cost = cost + e.getValue();
                    }
                }
                ret[row][col] = cost;
            }
        }

        log.info("------ buildCostMatrix: printing matrix -------");
        for(int i = 0; i<before.size();i++){
            log.info("row %s: %s", i, Arrays.toString(ret[i]));
        }

        return ret;
    }

    private static void printRoutingMaps(HashMap<Integer, HashMap<DataSegment, Integer>> map, String arg) {
        log.info("pringRoutingMap: [%s]", arg);
        for(Map.Entry<Integer, HashMap<DataSegment, Integer>> e : map.entrySet()){
            log.info("server: [%s] segmentList: ", e.getKey());
            for(Map.Entry<DataSegment, Integer> entry : e.getValue().entrySet()){
                log.info("[%s]: [%s]", entry.getKey().getIdentifier(), entry.getValue());
            }
        }
    }

    public static int[] hungarianMatching(int[][] m){
        Hungarian h = new Hungarian(m);
        return h.getResult();
    }


    public static HashMap<DataSegment,HashMap<DruidServerMetadata,Long>> rebuildRouting(HashMap<DataSegment, HashMap<DruidServerMetadata, Long>> routingTable, int[] hungarianMap, HashMap<DruidServerMetadata, Integer> metadataToIDMap, HashMap<Integer, DruidServerMetadata> IDToMetadataMap) {
        HashMap<DataSegment, HashMap<DruidServerMetadata,Long>> ret = new HashMap<DataSegment, HashMap<DruidServerMetadata, Long>>();
        for(Map.Entry<DataSegment, HashMap<DruidServerMetadata, Long>> entry : routingTable.entrySet()){
            ret.put(entry.getKey(), new HashMap<DruidServerMetadata, Long>());
            for(Map.Entry<DruidServerMetadata, Long> e : entry.getValue().entrySet()){
                int id = metadataToIDMap.get(e.getKey());
                int mappedID = hungarianMap[id];
                DruidServerMetadata mappedServerMetadata = IDToMetadataMap.get(mappedID);
                ret.get(entry.getKey()).put(mappedServerMetadata, e.getValue());
            }
        }
        return ret;

    }



    /**
     * The implementation of Hungarian algorithm in this class is adapted from Amir El Bawab
     * https://github.com/amirbawab/Hungarian-Algorithm.git
     */
    /**
     * Hungarian Algorithm is a combinatorial optimization algorithm that solves the assignment problem in polynomial time. Link: http://en.wikipedia.org/wiki/Hungarian_algorithm
     * Coded by Amir El Bawab
     * Date: 4 May 2014
     * License: MIT License ~ Please read License.txt for more information about the usage of this software
     * */
    private static class Hungarian {
        private int[][] originalValues; // Given values
        private int[][] values; // Cloned given values to be processed
        private int[][] lines; // Line drawn
        private int numLines; // Number of line drawn

        int rows[]; // Index of the column selected by every row (The final result)
        int occupiedCols[]; // Verify that all column are occupied, used in the optimization step

        public Hungarian(int[][] matrix) {
            // Initialization
            originalValues = matrix; // Given matrix
            values = cloneMatrix(matrix); // Cloned matrix to be processed
            rows = new int[values.length];
            occupiedCols = new int[values.length];

            //Algorithm
            subtractRowMinimal(); 				// Step 1
            subtractColMinimal();				// Step 2
            coverZeros();						// Step 3
            while(numLines < values.length){
                createAdditionalZeros();		// Step 4 (Condition)
                coverZeros();					// Step 3 Again (Condition)
            }
            optimization();						// Optimization
        }

        /**
         * Step 1
         * Subtract from every element the minimum value from its row
         * */
        public void subtractRowMinimal(){
            int rowMinValue[] = new int[values.length];
            //get the minimum for each row and store in rowMinValue[]
            for(int row=0; row<values.length;row++){
                rowMinValue[row] = values[row][0];
                for(int col=1; col<values.length;col++){
                    if(values[row][col] < rowMinValue[row])
                        rowMinValue[row] = values[row][col];
                }
            }

            //subtract minimum from each row using rowMinValue[]
            for(int row=0; row<values.length;row++){
                for(int col=0; col<values.length;col++){
                    values[row][col] -= rowMinValue[row];
                }
            }
        } //End Step 1

        /**
         * Step 2
         * Subtract from every element the minimum value from its column
         * */
        public void subtractColMinimal(){
            int colMinValue[] = new int[values.length];
            //get the minimum for each column and store them in colMinValue[]
            for(int col=0; col<values.length;col++){
                colMinValue[col] = values[0][col];
                for(int row=1; row<values.length;row++){
                    if(values[row][col] < colMinValue[col])
                        colMinValue[col] = values[row][col];
                }
            }

            //subtract minimum from each column using colMinValue[]
            for(int col=0; col<values.length;col++){
                for(int row=0; row<values.length;row++){
                    values[row][col] -= colMinValue[col];
                }
            }
        } //End Step 2

        /**
         * Step 3.1
         * Loop through all elements, and run colorNeighbors when the element visited is equal to zero
         * */
        public void coverZeros(){
            numLines = 0;
            lines = new int[values.length][values.length];

            for(int row=0; row<values.length;row++){
                for(int col=0; col<values.length;col++){
                    if(values[row][col] == 0)
                        colorNeighbors(row, col, maxVH(row, col));
                }
            }
        }

        /**
         * Step 3.2
         * Checks which direction (vertical,horizontal) contains more zeros, every time a zero is found vertically, we increment the result
         * and every time a zero is found horizontally, we decrement the result. At the end, result will be negative, zero or positive
         * @param row Row index for the target cell
         * @param col Column index for the target cell
         * @return Positive integer means that the line passing by indexes [row][col] should be vertical, Zero or Negative means that the line passing by indexes [row][col] should be horizontal
         * */
        private int maxVH(int row, int col){
            int result = 0;
            for(int i=0; i<values.length;i++){
                if(values[i][col] == 0)
                    result++;
                if(values[row][i] == 0)
                    result--;
            }
            return result;
        }

        /**
         * Step 3.3
         * Color the neighbors of the cell at index [row][col]. To know which direction to draw the lines, we pass maxVH value.
         * @param row Row index for the target cell
         * @param col Column index for the target cell
         * @param maxVH Value return by the maxVH method, positive means the line to draw passing by indexes [row][col] is vertical, negative or zero means the line to draw passing by indexes [row][col] is horizontal
         * */
        private void colorNeighbors(int row, int col, int maxVH){
            if(lines[row][col] == 2) // if cell is colored twice before (intersection cell), don't color it again
                return;

            if(maxVH > 0 && lines[row][col] == 1) // if cell colored vertically and needs to be recolored vertically, don't color it again (Allowing this step, will color the same line (result won't change), but the num of line will be incremented (wrong value for the num of line drawn))
                return;

            if(maxVH <= 0 && lines[row][col] == -1) // if cell colored horizontally and needs to be recolored horizontally, don't color it again (Allowing this step, will color the same line (result won't change), but the num of line will be incremented (wrong value for the num of line drawn))
                return;

            for(int i=0; i<values.length;i++){ // Loop on cell at indexes [row][col] and its neighbors
                if(maxVH > 0)	// if value of maxVH is positive, color vertically
                    lines[i][col] = lines[i][col] == -1 || lines[i][col] == 2 ? 2 : 1; // if cell was colored before as horizontal (-1), and now needs to be colored vertical (1), so this cell is an intersection (2). Else if this value was not colored before, color it vertically
                else			// if value of maxVH is zero or negative color horizontally
                    lines[row][i] = lines[row][i] == 1 || lines[row][i] == 2 ? 2 : -1; // if cell was colored before as vertical (1), and now needs to be colored horizontal (-1), so this cell is an intersection (2). Else if this value was not colored before, color it horizontally
            }

            // increment line number
            numLines++;
//		printMatrix(lines); // Monitor the line draw steps
        }//End step 3

        /**
         * Step 4
         * This step is not always executed. (Check the algorithm in the constructor)
         * Create additional zeros, by coloring the minimum value of uncovered cells (cells not colored by any line)
         * */
        public void createAdditionalZeros(){
            int minUncoveredValue = 0; // We don't know the value of the first uncovered cell, so we put a joker value 0 (0 is safe, because before this step, all zeros were covered)

            // Find the min in the uncovered numbers
            for(int row=0; row<values.length;row++){
                for(int col=0; col<values.length;col++){
                    if(lines[row][col] == 0 && (values[row][col] < minUncoveredValue || minUncoveredValue == 0))
                        minUncoveredValue = values[row][col];
                }
            }

            // Subtract min form all uncovered elements, and add it to all elements covered twice
            for(int row=0; row<values.length;row++){
                for(int col=0; col<values.length;col++){
                    if(lines[row][col] == 0) // If uncovered, subtract
                        values[row][col] -= minUncoveredValue;

                    else if(lines[row][col] == 2) // If covered twice, add
                        values[row][col] += minUncoveredValue;
                }
            }
        } // End step 4

        /**
         * Optimization, assign every row a cell in a unique column. Since a row can contain more than one zero,
         * we need to make sure that all rows are assigned one cell from one unique column. To do this, use brute force
         * @param row If all rows were assigned a cell from a unique column, return true (at the end, guarantee true)
         * @return true
         * */
        private boolean optimization(int row){
            if(row == rows.length) // If all rows were assigned a cell
                return true;

            for(int col=0; col<values.length;col++){ // Try all columns
                if(values[row][col] == 0 && occupiedCols[col] == 0){ // If the current cell at column `col` has a value of zero, and the column is not reserved by a previous row
                    rows[row] = col; // Assign the current row the current column cell
                    occupiedCols[col] = 1; // Mark the column as reserved
                    if(optimization(row+1)) // If the next rows were assigned successfully a cell from a unique column, return true
                        return true;
                    occupiedCols[col] = 0; // If the next rows were not able to get a cell, go back and try for the previous rows another cell from another column
                }
            }
            return false; // If no cell were assigned for the current row, return false to go back one row to try to assign to it another cell from another column
        }

        /**
         * Overload optimization(int row) method
         * @return true
         * */
        public boolean optimization(){
            return optimization(0);
        } //End optimization

        /**
         * Get the result by returning an array containing the cell assigned for each row
         * @return Array of rows where each array index represent the row number, and the value at each index is the column assigned to the corresponding row
         * */
        public int[] getResult(){
            return rows;
        }

        /**
         * Get the sum of the value of the assigned cells for all rows using the original passed matrix, and using the rows array to know the index of the column for each row.
         * @return Total values
         * */
        public int getTotal(){
            int total = 0;
            for(int row = 0; row < values.length; row++)
                total += originalValues[row][rows[row]];
            return total;
        }

        /**
         * Clone the 2D array
         * @return A copy of the 2D array
         * */
        public int[][] cloneMatrix(int[][] matrix){
            int[][] tmp = new int[matrix.length][matrix.length];
            for(int row = 0; row < matrix.length; row++){
                tmp[row] = matrix[row].clone();
            }
            return tmp;
        }

        /**
         * Print a 2D array
         * @param matrix The target 2D array
         * */
        public void printMatrix(int[][] matrix){
            for(int row=0; row<matrix.length;row++){
                for(int col=0; col<matrix.length;col++){
                    System.out.print(matrix[row][col]+"\t");
                }
                System.out.println();
            }
            System.out.println();
        }
    }


}

