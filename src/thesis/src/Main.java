package thesis.src;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import io.github.twalgor.main.ExactTW;

public class Main {

    /* ---------- 目录常量 ---------- */
    private static final String CSV_DIR   = "./csv_inputs";
    private static final String FD_DIR    = "./fd";
    private static final String DC_DIR    = "./dc";
    private static final String QUERY_DIR = "./q";
    private static final String OUT_DIR   = "./result";

    /* ---------- Fact ---------- */
    static class Fact {
        final Map<String,String> map = new HashMap<>();
        Fact(String[] header, String[] values){
            for(int i=0;i<header.length;i++)
                map.put(header[i].trim(),
                        i < values.length ? values[i].trim() : null);
        }
        String get(String k){ return map.get(k); }
    }

    /* ---------- DC Atom ---------- */
    static class DCAtom{
        String lAttr, op, rAttr, constVal;
        boolean isConst, sameTuple;            // sameTuple=true → t1.A op t1.B
        // t1.A op t2.B
        DCAtom(String l,String o,String r){
            lAttr=l; op=o; rAttr=r;
            isConst=false; sameTuple=false;
        }
        // t1.A op CONST  或  t1.A op t1.B
        DCAtom(String l,String o,String rhs, boolean isConst, boolean same){
            lAttr=l; op=o; this.isConst=isConst; this.sameTuple=same;
            if(isConst) constVal=rhs; else rAttr=rhs;
        }
    }

    /* ---------- CSV ---------- */
    static List<Fact> readFacts(Path csv) throws IOException{
        List<Fact> list=new ArrayList<>();
        try(BufferedReader br=Files.newBufferedReader(csv)){
            String header=br.readLine(); if(header==null) return list;
            String[] h = header.split(",");
            // 去掉列名中的引号
            for(int i = 0; i < h.length; i++) {
                h[i] = h[i].trim();
                if(h[i].startsWith("\"") && h[i].endsWith("\"")) {
                    h[i] = h[i].substring(1, h[i].length()-1);
                }
            }

            System.out.println("CSV Headers after cleaning (" + h.length + "):");
            for(int i = 0; i < h.length; i++) {
                System.out.println("  [" + i + "]: '" + h[i] + "'");
            }

            String line;
            int rowCount = 0;
            while((line=br.readLine())!=null) {
                String[] values = line.split(",");
                // 去掉值中的引号
                for(int i = 0; i < values.length; i++) {
                    values[i] = values[i].trim();
                    if(values[i].startsWith("\"") && values[i].endsWith("\"")) {
                        values[i] = values[i].substring(1, values[i].length()-1);
                    }
                }
                list.add(new Fact(h, values));
                rowCount++;
            }
            System.out.println("Total data rows: " + rowCount);
        }
        return list;
    }

    /* ---------- FD ---------- */
    static List<Map.Entry<List<String>,String>> readFD(Path fd) throws IOException{
        List<Map.Entry<List<String>,String>> list=new ArrayList<>();
        if(!Files.exists(fd)) return list;
        try(BufferedReader br=Files.newBufferedReader(fd)){
            String l;
            while((l=br.readLine())!=null){
                l=l.trim();
                if(l.isEmpty()||l.startsWith("#")) continue;
                String[] parts=l.split("->");
                if(parts.length!=2) continue;
                List<String> lhsList = new ArrayList<>();
                String lhsStr = parts[0].trim();
                if(!lhsStr.isEmpty()) {
                    for(String attr : lhsStr.split(",")) {
                        String trimmedAttr = attr.trim();
                        if(!trimmedAttr.isEmpty()) {
                            lhsList.add(trimmedAttr);
                        }
                    }
                }
                if(!lhsList.isEmpty()) {
                    list.add(new AbstractMap.SimpleEntry<>(lhsList, parts[1].trim()));
                }
            }
        }
        return list;
    }
    static boolean violatesFD(List<String> lhs,String rhs,Fact f1,Fact f2){
        for(String a: lhs){
            String v1=f1.get(a), v2=f2.get(a);
            if(v1 == null || v2 == null || !v1.equals(v2)) return false;
        }
        String r1=f1.get(rhs), r2=f2.get(rhs);
        return r1!=null && r2!=null && !r1.equals(r2);
    }

    /* ---------- DC ---------- */
    static List<List<DCAtom>> readDC(Path dc) throws IOException{
        List<List<DCAtom>> dcs=new ArrayList<>();
        if(!Files.exists(dc)) return dcs;

        // 修复正则表达式，改善解析
        Pattern pat = Pattern.compile(
                "(t[12])\\.([^\\s!=><]+)\\s*([!=><]+)\\s*" +
                "(?:(t[12])\\.([^\\s!=><]+)|" +  // 另一个元组的属性
                "([0-9.-]+)|" +                  // 数字常量
                "\"([^\"]*)\"|" +               // 双引号字符串
                "'([^']*)')"                    // 单引号字符串
        );

        try(BufferedReader br=Files.newBufferedReader(dc)){
            String l;
            while((l=br.readLine())!=null){
                String original = l.trim();
                if(original.isEmpty()||original.startsWith("#")) continue;

                // 预处理：去掉 ¬( )
                String processed = original;
                if(processed.startsWith("¬(") && processed.endsWith(")")) {
                    processed = processed.substring(2, processed.length()-1);
                }

                System.out.println("Processing DC: " + original);
                System.out.println("After preprocessing: " + processed);

                List<DCAtom> clause=new ArrayList<>();

                // 按 && 分割原子
                String[] atoms = processed.split("&&");

                for(String atomStr : atoms) {
                    atomStr = atomStr.trim();
                    Matcher m = pat.matcher(atomStr);

                    if(m.find()) {
                        String leftTuple = m.group(1);    // t1 或 t2
                        String leftAttr = m.group(2);     // 属性名
                        String op = m.group(3);           // 操作符

                        System.out.println("  Found atom - " + leftTuple + "." + leftAttr + " " + op);

                        if(m.group(4) != null) {          // 右边是另一个元组的属性
                            String rightTuple = m.group(4);
                            String rightAttr = m.group(5);
                            boolean sameTuple = leftTuple.equals(rightTuple);
                            System.out.println("    Right: " + rightTuple + "." + rightAttr + " (same tuple: " + sameTuple + ")");
                            clause.add(new DCAtom(leftAttr, op, rightAttr, false, sameTuple));
                        }
                        else if(m.group(6) != null) {     // 数字常量
                            String constVal = m.group(6);
                            System.out.println("    Constant (numeric): " + constVal);
                            clause.add(new DCAtom(leftAttr, op, constVal, true, false));
                        }
                        else if(m.group(7) != null) {     // 双引号字符串常量
                            String constVal = m.group(7);
                            System.out.println("    Constant (string): \"" + constVal + "\"");
                            clause.add(new DCAtom(leftAttr, op, constVal, true, false));
                        }
                        else if(m.group(8) != null) {     // 单引号字符串常量
                            String constVal = m.group(8);
                            System.out.println("    Constant (string): '" + constVal + "'");
                            clause.add(new DCAtom(leftAttr, op, constVal, true, false));
                        }
                    } else {
                        System.out.println("  WARNING: Could not parse atom: " + atomStr);
                    }
                }

                if(!clause.isEmpty()) {
                    dcs.add(clause);
                    System.out.println("  Added DC with " + clause.size() + " atoms");
                } else {
                    System.out.println("  WARNING: No atoms parsed from DC line!");
                }
            }
        }
        System.out.println("Total DCs loaded: " + dcs.size());
        return dcs;
    }

    static int cmpNum(String a,String b){
        try{ return Double.compare(Double.parseDouble(a),Double.parseDouble(b)); }
        catch(Exception e){ return a.compareTo(b); }
    }

    static boolean sat(DCAtom a,Fact f1,Fact f2){
        String v1=f1.get(a.lAttr); if(v1==null) return false;
        if(a.isConst){
            String c=a.constVal;
            return switch(a.op){
                case "="  -> v1.equals(c);
                case "!=" -> !v1.equals(c);
                case ">"  -> cmpNum(v1,c)>0;
                case "<"  -> cmpNum(v1,c)<0;
                case ">=" -> cmpNum(v1,c)>=0;
                case "<=" -> cmpNum(v1,c)<=0;
                default   -> false;
            };
        }else{
            String v2=a.sameTuple ? f1.get(a.rAttr) : f2.get(a.rAttr);
            if(v2==null) return false;
            return switch(a.op){
                case "="  -> v1.equals(v2);
                case "!=" -> !v1.equals(v2);
                case ">"  -> cmpNum(v1,v2)>0;
                case "<"  -> cmpNum(v1,v2)<0;
                case ">=" -> cmpNum(v1,v2)>=0;
                case "<=" -> cmpNum(v1,v2)<=0;
                default   -> false;
            };
        }
    }

    static boolean violatesDC(List<DCAtom> clause,Fact f1,Fact f2){
        // DC违反：当所有原子都为真时，DC被违反
        for(DCAtom a: clause) {
            if(!sat(a,f1,f2)) {
                return false;
            }
        }
        return true; // 所有原子都满足，DC被违反
    }

    /* ---------- Tree-width utils ---------- */
    static int readTw(Path td) throws IOException{
        int max=0;
        try(BufferedReader br=Files.newBufferedReader(td)){
            String l;
            while((l=br.readLine())!=null){
                if(l.startsWith("b "))
                    max = Math.max(max, l.trim().split(" ").length - 2);
            }
        }
        return Math.max(0, max-1);
    }
    static void writeTw(int tw,Path out) throws IOException{
        Files.createDirectories(out.getParent());
        try(BufferedWriter bw=Files.newBufferedWriter(out)){
            bw.write("Treewidth = " + tw); bw.newLine();
        }
    }

    /* --- 极小满足集查找 --- */
    static List<Set<Integer>> findMinimalSatisfyingSets(List<Fact> facts, Path queryPath) throws IOException{
        List<Set<Integer>> minimalSets = new ArrayList<>();
        if(!Files.exists(queryPath)) {
            System.out.println("Query file does not exist: " + queryPath);
            return minimalSets;
        }

        Map<String, Set<String>> queryConditions = new HashMap<>();
        try(BufferedReader br = Files.newBufferedReader(queryPath)){
            String l;
            while((l = br.readLine()) != null){
                l = l.trim();
                if(l.isEmpty() || l.startsWith("#") || !l.contains("=")) continue;

                String[] parts = l.split("=", 2);
                if(parts.length == 2) {
                    String attribute = parts[0].trim();
                    String valuesStr = parts[1].trim();

                    if(!attribute.isEmpty() && !valuesStr.isEmpty()) {
                        Set<String> allowedValues = new HashSet<>();
                        // 支持逗号分隔的多个值
                        for(String val : valuesStr.split(",")) {
                            String trimmedVal = val.trim();
                            // 去掉引号
                            if((trimmedVal.startsWith("\"") && trimmedVal.endsWith("\"")) ||
                               (trimmedVal.startsWith("'") && trimmedVal.endsWith("'"))) {
                                trimmedVal = trimmedVal.substring(1, trimmedVal.length()-1);
                            }
                            if(!trimmedVal.isEmpty()) {
                                allowedValues.add(trimmedVal);
                            }
                        }
                        if(!allowedValues.isEmpty()) {
                            queryConditions.put(attribute, allowedValues);
                            System.out.println("Query condition: " + attribute + " ∈ " + allowedValues);
                        }
                    }
                }
            }
        }

        if(queryConditions.isEmpty()) {
            System.out.println("No valid query conditions found");
            return minimalSets;
        }

        System.out.println("Searching for minimal satisfying sets...");

        // 按大小递增搜索极小满足集
        int n = facts.size();
        boolean foundAny = false;

        for(int sz = 1; sz <= n && !foundAny; sz++){
            System.out.println("  Checking sets of size " + sz + "...");
            List<Set<Integer>> currentSizeResults = new ArrayList<>();
            generateCombinations(facts, queryConditions, sz, 0, new ArrayList<>(), currentSizeResults);

            if(!currentSizeResults.isEmpty()) {
                foundAny = true;
                minimalSets.addAll(currentSizeResults);
                System.out.println("  Found " + currentSizeResults.size() + " minimal sets of size " + sz);
            }
        }

        return minimalSets;
    }

    static void generateCombinations(List<Fact> facts, Map<String, Set<String>> queryConditions,
                                   int targetSize, int startIndex, List<Integer> current,
                                   List<Set<Integer>> results){
        if(current.size() == targetSize){
            if(satisfiesAllConditions(facts, current, queryConditions)){
                results.add(new HashSet<>(current));
            }
            return;
        }

        for(int i = startIndex; i < facts.size(); i++){
            current.add(i);
            generateCombinations(facts, queryConditions, targetSize, i+1, current, results);
            current.remove(current.size()-1);
        }
    }

    static boolean satisfiesAllConditions(List<Fact> facts, List<Integer> indices,
                                        Map<String, Set<String>> queryConditions){
        // 对每个查询条件，检查选定的事实是否至少有一个满足该条件
        for(String attribute : queryConditions.keySet()){
            Set<String> requiredValues = queryConditions.get(attribute);
            boolean foundMatchingValue = false;

            for(Integer idx : indices){
                Fact fact = facts.get(idx);
                String actualValue = fact.get(attribute);
                if(actualValue != null && requiredValues.contains(actualValue)) {
                    foundMatchingValue = true;
                    break;
                }
            }

            if(!foundMatchingValue) {
                return false; // 该条件没有被任何选定的事实满足
            }
        }
        return true; // 所有条件都被满足
    }

    /* --- 生成解-冲突图 --- */
    static void writeGr(List<Fact> facts,
                        List<Map.Entry<List<String>,String>> fds,
                        List<List<DCAtom>> dcs,
                        Path out,
                        List<Set<Integer>> minimalSatisfyingSets) throws IOException{

        Set<String> edgeSet = new HashSet<>();  // 去重边
        List<int[]> edges = new ArrayList<>();

        System.out.println("Generating solution-conflict graph...");
        System.out.println("  Facts: " + facts.size());
        System.out.println("  FDs: " + fds.size());
        System.out.println("  DCs: " + dcs.size());
        System.out.println("  Minimal satisfying sets: " + minimalSatisfyingSets.size());

        // 输出每个极小满足集的详细信息
        for(int i = 0; i < minimalSatisfyingSets.size(); i++) {
            System.out.println("    Set " + (i+1) + ": " + minimalSatisfyingSets.get(i));
        }

        // 1. 添加冲突边：来自FD和DC违反
        System.out.println("  Adding conflict edges...");
        int conflictEdges = 0;

        for(int i = 0; i < facts.size(); i++){
            for(int j = i + 1; j < facts.size(); j++){
                Fact f1 = facts.get(i);
                Fact f2 = facts.get(j);
                boolean hasConflict = false;

                // 检查 FD 违反
                for(var fd : fds){
                    if(violatesFD(fd.getKey(), fd.getValue(), f1, f2)){
                        hasConflict = true;
                        System.out.println("    FD violation between fact " + i + " and " + j);
                        break;
                    }
                }

                // 如果还没有冲突，检查 DC 违反
                if(!hasConflict) {
                    for(var dc : dcs){
                        if(violatesDC(dc, f1, f2)){
                            hasConflict = true;
                            System.out.println("    DC violation between fact " + i + " and " + j);
                            break;
                        }
                    }
                }

                if(hasConflict){
                    int id1 = i + 1; // 转换为1基索引
                    int id2 = j + 1;
                    String edgeKey = Math.min(id1, id2) + "," + Math.max(id1, id2);
                    if(!edgeSet.contains(edgeKey)) {
                        edgeSet.add(edgeKey);
                        edges.add(new int[]{Math.min(id1, id2), Math.max(id1, id2)});
                        conflictEdges++;
                    }
                }
            }
        }
        System.out.println("  Added " + conflictEdges + " conflict edges");

        // 2. 添加解决方案边：每个极小满足集内的所有节点对之间添加边（形成完全子图）
        System.out.println("  Adding solution edges...");
        int solutionEdges = 0;

        for(int setIdx = 0; setIdx < minimalSatisfyingSets.size(); setIdx++){
            Set<Integer> minSet = minimalSatisfyingSets.get(setIdx);
            List<Integer> nodeList = new ArrayList<>(minSet);

            System.out.println("    Processing minimal set " + (setIdx + 1) + " with " + nodeList.size() + " nodes");

            // 在极小满足集内的每对节点之间添加边
            for(int i = 0; i < nodeList.size(); i++){
                for(int j = i + 1; j < nodeList.size(); j++){
                    int id1 = nodeList.get(i) + 1;  // 转换为1基索引
                    int id2 = nodeList.get(j) + 1;
                    String edgeKey = Math.min(id1, id2) + "," + Math.max(id1, id2);

                    if(!edgeSet.contains(edgeKey)) {
                        edgeSet.add(edgeKey);
                        edges.add(new int[]{Math.min(id1, id2), Math.max(id1, id2)});
                        solutionEdges++;
                        System.out.println("      Added solution edge: " + Math.min(id1, id2) + " - " + Math.max(id1, id2));
                    }
                }
            }
        }
        System.out.println("  Added " + solutionEdges + " solution edges");
        System.out.println("  Total unique edges: " + edges.size());

        // 写入图文件
        Files.createDirectories(out.getParent());
        try(BufferedWriter bw = Files.newBufferedWriter(out)){
            int n = facts.size();
            bw.write("p tw " + n + " " + edges.size());
            bw.newLine();

            // 按字典序排序边，确保输出一致性
            edges.sort((a, b) -> {
                int cmp = Integer.compare(a[0], b[0]);
                return cmp != 0 ? cmp : Integer.compare(a[1], b[1]);
            });

            for(int[] edge : edges) {
                bw.write(edge[0] + " " + edge[1]);
                bw.newLine();
            }
        }

        System.out.println("Solution-conflict graph written to: " + out);
    }

    /* ---------- Main ---------- */
    public static void main(String[] args) throws Exception{
        Files.createDirectories(Paths.get(OUT_DIR));

        File[] csvFiles = new File(CSV_DIR).listFiles((_, name) -> name.toLowerCase().endsWith(".csv"));
        if(csvFiles == null || csvFiles.length == 0){
            System.err.println("No CSV files found in " + CSV_DIR);
            return;
        }

        for(File csv : csvFiles){
            String baseName = csv.getName().replace(".csv", "");
            System.out.println("\n" + "=".repeat(50));
            System.out.println("Processing: " + baseName);
            System.out.println("=".repeat(50));

            // 读取数据
            List<Fact> facts = readFacts(csv.toPath());
            var fds = readFD(Path.of(FD_DIR, baseName + ".fd"));
            var dcs = readDC(Path.of(DC_DIR, baseName + ".dc"));

            // 查找极小满足集
            List<Set<Integer>> minimalSatisfyingSets = findMinimalSatisfyingSets(
                facts, Path.of(QUERY_DIR, baseName + ".q"));

            System.out.println("Found " + minimalSatisfyingSets.size() + " minimal satisfying sets");

            // 生成解-冲突图
            Path graphPath = Path.of(OUT_DIR, baseName + "_solution_conflict_graph.gr");
            writeGr(facts, fds, dcs, graphPath, minimalSatisfyingSets);

            // 计算树宽
            Path treeDecompositionPath = Path.of(OUT_DIR, baseName + "_result.td");
            ExactTW.main(new String[]{graphPath.toString(), treeDecompositionPath.toString(), "-acsd"});
            writeTw(readTw(treeDecompositionPath), Path.of(OUT_DIR, baseName + "_treewidth.txt"));

            System.out.println("Completed processing: " + baseName);
        }
        System.out.println("\nAll files processed successfully!");
    }
}


