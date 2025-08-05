package thesis.src;/*  Main.java – v7  (完整版本)
 *  -----------------------------------------------------------
 *  DC 原子支持:
 *      ① t1.A op t2.B     (跨元组)
 *      ② t1.A op CONST    (常量比较)
 *      ③ t1.A op t1.B     (同元组列间比较)
 *  op ∈ { = , != , > , < }
 *
 *  路径结构:
 *      csv_inputs/   *.csv
 *      fd/           *.fd
 *      dc/           *.dc
 *      query/        *.query
 *      result/       *.gr  *.td  *_treewidth.txt
 *
 *  依赖:
 *      JDK ≥ 11
 *      exacttw.jar  (PACE 2017 tw-solver)
 */
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
    private static final String QUERY_DIR = "./query";
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
            String line;
            while((line=br.readLine())!=null)
                list.add(new Fact(h, line.split(",")));
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
                list.add(new AbstractMap.SimpleEntry<>(
                        Arrays.asList(parts[0].trim().split(",")),
                        parts[1].trim()));
            }
        }
        return list;
    }
    static boolean violatesFD(List<String> lhs,String rhs,Fact f1,Fact f2){
        for(String a: lhs){
            String v1=f1.get(a), v2=f2.get(a);
            if(v1 == null || !v1.equals(v2)) return false;
        }
        String r1=f1.get(rhs), r2=f2.get(rhs);
        return r1!=null && r2!=null && !r1.equals(r2);
    }

    /* ---------- DC ---------- */
    static List<List<DCAtom>> readDC(Path dc) throws IOException{
        List<List<DCAtom>> dcs=new ArrayList<>();
        if(!Files.exists(dc)) return dcs;

        // group(3)=t2.attr  group(4)=t1.attr  group(5)=num const  group(6)=str const
        Pattern pat = Pattern.compile(
                "t1\\.([A-Za-z0-9_ ()%-]+)([!=><]{1,2})(?:t2\\.([A-Za-z0-9_ ()%-]+)|t1\\.([A-Za-z0-9_ ()%-]+)|([0-9.]+)|\"([^\"]+)\")"
        );

        try(BufferedReader br=Files.newBufferedReader(dc)){
            String l;
            while((l=br.readLine())!=null){
                l=l.replaceAll("\\s+","");           // 去空白
                if(l.isEmpty()||l.startsWith("#")) continue;
                if(l.startsWith("¬(")) l=l.substring(2,l.length()-1); // 去掉 ¬( )
                List<DCAtom> clause=new ArrayList<>();
                for(String at:l.split("&&")){
                    Matcher m=pat.matcher(at);
                    if(m.matches()){
                        String left=m.group(1), op=m.group(2);
                        if(m.group(3)!=null)      // t1 vs t2
                            clause.add(new DCAtom(left,op,m.group(3)));
                        else if(m.group(4)!=null) // t1 vs t1
                            clause.add(new DCAtom(left,op,m.group(4),false,true));
                        else if(m.group(5)!=null) // numeric const
                            clause.add(new DCAtom(left,op,m.group(5),true,false));
                        else if(m.group(6)!=null) // string const
                            clause.add(new DCAtom(left,op,m.group(6),true,false));
                    }
                }
                if(!clause.isEmpty()) dcs.add(clause);
            }
        }
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
                default   -> false;
            };
        }
    }
    static boolean violatesDC(List<DCAtom> clause,Fact f1,Fact f2){
        for(DCAtom a: clause) if(!sat(a,f1,f2)) return false;
        return true;
    }

//    /* ---------- Query ---------- */
//    static Set<Integer> queryIdx(List<Fact> facts,Path q) throws IOException{
//        Set<Integer> idx=new HashSet<>();
//        if(!Files.exists(q)) return idx;
//        Map<String,Set<String>> cond=new HashMap<>();
//        try(BufferedReader br=Files.newBufferedReader(q)){
//            String l;
//            while((l=br.readLine())!=null){
//                if(!l.contains("=")) continue;
//                String[] p=l.split("=");
//                cond.put(p[0].trim(),
//                        new HashSet<>(Arrays.asList(p[1].trim().split(","))));
//            }
//        }
//        for(int i=0;i<facts.size();i++){
//            Fact f=facts.get(i); boolean ok=true;
//            for(String k:cond.keySet()){
//                String v=f.get(k);
//                if(v==null||!cond.get(k).contains(v)){ ok=false; break; }
//            }
//            if(ok) idx.add(i);
//        }
//        return idx;
//    }

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

    /* --- 新增: 极小满足集查找 --- */
    static List<Set<Integer>> findMinimalSatisfyingSets(List<Fact> facts, Path queryPath) throws IOException{
        List<Set<Integer>> minimalSets = new ArrayList<>();
        if(!Files.exists(queryPath)) return minimalSets;

        Map<String, Set<String>> cond = new HashMap<>();
        try(BufferedReader br = Files.newBufferedReader(queryPath)){
            String l;
            while((l = br.readLine()) != null){
                if(!l.contains("=")) continue;
                String[] p = l.split("=");
                cond.put(p[0].trim(), new HashSet<>(Arrays.asList(p[1].trim().split(","))));
            }
        }

        int n = facts.size();
        for(int sz = 1; sz <= n; sz++){
            combine(facts, cond, sz, 0, new ArrayList<>(), minimalSets);
        }
        return minimalSets;
    }

    static void combine(List<Fact> facts, Map<String, Set<String>> cond, int sz, int start, List<Integer> curr, List<Set<Integer>> result){
        if(curr.size() == sz){
            if(satisfiesQuery(facts, curr, cond)){
                boolean minimal = true;
                for(int i = 0; i < curr.size(); i++){
                    List<Integer> sub = new ArrayList<>(curr);
                    sub.remove(i);
                    if(satisfiesQuery(facts, sub, cond)){
                        minimal = false;
                        break;
                    }
                }
                if(minimal) result.add(new HashSet<>(curr));
            }
            return;
        }
        for(int i = start; i < facts.size(); i++){
            curr.add(i);
            combine(facts, cond, sz, i+1, curr, result);
            curr.remove(curr.size()-1);
        }
    }

    static boolean satisfiesQuery(List<Fact> facts, List<Integer> indices, Map<String, Set<String>> cond){
        Map<String, Set<String>> values = new HashMap<>();
        for(Integer idx : indices){
            Fact f = facts.get(idx);
            for(String k : cond.keySet()){
                values.computeIfAbsent(k, _ -> new HashSet<>()).add(f.get(k));
            }
        }
        for(String k : cond.keySet()){
            if(Collections.disjoint(values.getOrDefault(k, Set.of()), cond.get(k)))
                return false;
        }
        return true;
    }

    /* --- 修改 writeGr 增加“解决方案边” --- */
    static void writeGr(List<Fact> facts,
                        List<Map.Entry<List<String>,String>> fds,
                        List<List<DCAtom>> dcs,
                        Path out,
                        List<Set<Integer>> solutionEdges) throws IOException{

        List<Fact> tg = facts;
        List<Integer> map = null;

        List<int[]> edges = new ArrayList<>();

        // 冲突边
        for(int i = 0; i < tg.size(); i++){
            for(int j = i+1; j < tg.size(); j++){
                Fact f1 = tg.get(i), f2 = tg.get(j);
                boolean conf = false;

                for(var fd : fds){
                    if(violatesFD(fd.getKey(), fd.getValue(), f1, f2)){ conf = true; break; }
                }
                if(!conf){
                    for(var dc : dcs){
                        if(violatesDC(dc, f1, f2)){ conf = true; break; }
                    }
                }
                if(conf){
                    int id1 = i+1;
                    int id2 = j+1;
                    edges.add(new int[]{id1, id2});
                }
            }
        System.out.println("Final Result: " + edges);
        }

        // 解决方案边 (极小满足集)
        for(Set<Integer> sol : solutionEdges){
            List<Integer> list = new ArrayList<>(sol);
            for(int i = 0; i < list.size(); i++){
                for(int j = i+1; j < list.size(); j++){
                    edges.add(new int[]{list.get(i)+1, list.get(j)+1});
                }
            }
        }

        Files.createDirectories(out.getParent());
        try(BufferedWriter bw = Files.newBufferedWriter(out)){
            int n = facts.size();
            bw.write("p tw " + n + " " + edges.size()); bw.newLine();
            for(int[] e : edges) bw.write(e[0] + " " + e[1] + "\n");
        }
    }

    /* --- Main 调用修改 --- */
    public static void main(String[] args) throws Exception{

        Files.createDirectories(Paths.get(OUT_DIR));
        File[] csvFiles = new File(CSV_DIR)
                .listFiles((_, n) -> n.toLowerCase().endsWith(".csv"));
        if(csvFiles == null || csvFiles.length == 0){
            System.err.println("No CSV in " + CSV_DIR);
            return;
        }

        for(File csv : csvFiles){
            String base = csv.getName().replace(".csv", "");
            List<Fact> facts = readFacts(csv.toPath());
            var fds = readFD(Path.of(FD_DIR, base + ".fd"));
            var dcs = readDC(Path.of(DC_DIR, base + ".dc"));

            List<Set<Integer>> solutionEdges = findMinimalSatisfyingSets(facts, Path.of(QUERY_DIR, base + ".query"));

            Path g = Path.of(OUT_DIR, base + "_solution_conflict_graph.gr");
            writeGr(facts, fds, dcs, g, solutionEdges);

            Path td = Path.of(OUT_DIR, base + "_result.td");
            ExactTW.main(new String[]{g.toString(), td.toString(), "-acsd"});
            writeTw(readTw(td), Path.of(OUT_DIR, base + "_treewidth.txt"));
        }
    }
}





