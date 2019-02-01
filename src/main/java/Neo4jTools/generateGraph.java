package Neo4jTools;

import javafx.util.Pair;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.*;

public class generateGraph {
    String DBBase = "/home/gqxwolf/mydata/projectData/testGraph10_2/data/";
    String EdgesPath = DBBase + "SegInfo.txt";
    String NodePath = DBBase + "NodeInfo.txt";

    int numberNodes, numberofEdges, numberofDimens;


    public generateGraph(int graphsize, int degree, int dimensions) {
        this.numberNodes = graphsize;
        this.numberofEdges = Math.round(numberNodes * (degree));
        this.numberofDimens = dimensions;

        this.DBBase = "/home/gqxwolf/mydata/projectData/BackBone/testRandomGraph_" + graphsize + "_" + degree + "/data/";
        EdgesPath = DBBase + "SegInfo.txt";
        NodePath = DBBase + "NodeInfo.txt";

    }


    public static void main(String args[]) throws ParseException {
        int numberNodes, numberofDegree, numberofDimen;


        Options options = new Options();
        options.addOption("g", "grahpsize", true, "number of nodes in the graph");
        options.addOption("de", "degree", true, "degree of the graphe");
        options.addOption("di", "dimension", true, "dimension of the graph");
        options.addOption("h", "help", false, "print the help of this command");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String g_str = cmd.getOptionValue("g");
        String de_str = cmd.getOptionValue("de");
        String di_str = cmd.getOptionValue("di");


        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            String header = "Run the code to generate the graph :";
            formatter.printHelp("java -jar generateGraph.jar", header, options, "", false);
        } else {

            if (g_str == null) {
                numberNodes = 1000;
            } else {
                numberNodes = Integer.parseInt(g_str);
            }

            if (de_str == null) {
                numberofDegree = 4;
            } else {
                numberofDegree = Integer.parseInt(de_str);
            }

            if (di_str == null) {
                numberofDimen = 3;
            } else {
                numberofDimen = Integer.parseInt(di_str);
            }

            generateGraph g = new generateGraph(numberNodes, numberofDegree, numberofDimen);
            g.generateG(true);
        }
    }

    public void generateG(boolean deleteBefore) {
        if (deleteBefore) {
            File dataF = new File(DBBase);
            try {
                FileUtils.deleteDirectory(dataF);
                dataF.mkdirs();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        HashMap<Pair<String, String>, String[]> Edges = new HashMap<>();
        HashMap<String, String[]> Nodes = new HashMap<>();


        //生成经度和纬度
        for (int i = 0; i < numberNodes; i++) {
            String cost1 = String.valueOf(getRandomNumberInRange(1, 360));
            String cost2 = String.valueOf(getRandomNumberInRange(1, 360));
            Nodes.put(String.valueOf(i), new String[]{cost1, cost2});
        }

        //Create the Edges information.
        for (int i = 0; i < numberofEdges; i++) {
            String startNode = String.valueOf(getRandomNumberInRange_int(0, numberNodes - 1));
            String endNode = String.valueOf(getRandomNumberInRange_int(0, numberNodes - 1));
            while (startNode.equals(endNode)) {
                endNode = String.valueOf(getRandomNumberInRange_int(0, numberNodes - 1));
            }

            double x1 = Double.valueOf(Nodes.get(startNode)[0]);
            double y1 = Double.valueOf(Nodes.get(startNode)[1]);
            double x2 = Double.valueOf(Nodes.get(endNode)[0]);
            double y2 = Double.valueOf(Nodes.get(endNode)[1]);

            double dist = Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));

            String[] costs = new String[numberofDimens];
            for (int j = 0; j < numberofDimens; j++) {
                costs[j] = String.valueOf(getGaussian(dist, dist * 0.2));
            }


            Edges.put(new Pair(startNode, endNode), costs);
        }

        //stores all of the start node of the created edges
        HashSet<String> containedNodes = new HashSet<>();
        for (Pair<String, String> p : Edges.keySet()) {
            containedNodes.add(p.getKey());
        }

        for (String node : Nodes.keySet()) {
            //if there is the node does not have any edge start with it
            //Create edge for it
            if (!containedNodes.contains(node)) {
                String startNode = String.valueOf(node);
                String endNode = String.valueOf(getRandomNumberInRange_int(0, numberNodes - 1));
                while (startNode.equals(endNode)) {
                    endNode = String.valueOf(getRandomNumberInRange_int(0, numberNodes - 1));
                }

                double x1 = Double.valueOf(Nodes.get(startNode)[0]);
                double y1 = Double.valueOf(Nodes.get(startNode)[1]);
                double x2 = Double.valueOf(Nodes.get(endNode)[0]);
                double y2 = Double.valueOf(Nodes.get(endNode)[1]);

                double dist = Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));

                String[] costs = new String[numberofDimens];
                for (int j = 0; j < numberofDimens; j++) {
                    costs[j] = String.valueOf(getGaussian(dist * 2, dist * 0.3));
                }

                Edges.put(new Pair(startNode, endNode), costs);
            }
        }

        writeNodeToDisk(Nodes);
        writeEdgeToDisk(Edges);
        System.out.println("Graph is created, there are " + Nodes.size() + " nodes and " + Edges.size() + " edges");


    }

    private void writeEdgeToDisk(HashMap<Pair<String, String>, String[]> edges) {
        try (FileWriter fw = new FileWriter(EdgesPath, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            for (Map.Entry<Pair<String, String>, String[]> node : edges.entrySet()) {
//                System.out.println(EdgesPath);
                StringBuffer sb = new StringBuffer();
                String snodeId = node.getKey().getKey();
                String enodeId = node.getKey().getValue();
                sb.append(snodeId).append(" ");
                sb.append(enodeId).append(" ");
                for (String cost : node.getValue()) {
                    sb.append(cost).append(" ");
                }
                out.println(sb.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeNodeToDisk(HashMap<String, String[]> nodes) {
        try (FileWriter fw = new FileWriter(NodePath, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            System.out.println(NodePath);
            TreeMap<String, String[]> tm = new TreeMap<String, String[]>(new StringComparator());
            tm.putAll(nodes);
            for (Map.Entry<String, String[]> node : tm.entrySet()) {
                StringBuffer sb = new StringBuffer();
                String nodeId = node.getKey();
                sb.append(nodeId).append(" ");
                for (String cost : node.getValue()) {
                    sb.append(cost).append(" ");
                }
                out.println(sb.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private double getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextDouble() * (max - min) + min;
    }


    private int getRandomNumberInRange_int(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }

    private double getGaussian(double mean, double sd) {
        Random r = new Random();
        double value = r.nextGaussian() * sd + mean;

        while (value <= 0) {
            value = r.nextGaussian() * sd + mean;
        }

        return value;
    }


}
