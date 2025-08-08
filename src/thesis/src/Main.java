package thesis.src;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import io.github.twalgor.main.ExactTW;

public class Main {

    private static final String CSV_DIR   = "./csv_inputs";
    private static final String FD_DIR    = "./fd";
    private static final String DC_DIR    = "./dc";
    private static final String QUERY_DIR = "./query";
    private static final String OUT_DIR   = "./result";

    /* ------------------------------
     *           Data model
     * ------------------------------ */

    static class Fact {
        final Map<String,String> map = new HashMap<>();
        Fact(String[] header, String[] values){
            for(int i=0;i<header.length;i++){
                String v = (i<values.length ? values[i] : null);
                map.put(header[i].trim(), v==null? null : v.trim());
            }
        }
        String get(String k){ return map.get(k); }
    }

    /** FD: 支持多 RHS 属性。 */
    static class FD {
        final List<String> lhs;
        final List<String> rhs;
        FD(List<String> lhs, List<String> rhs){ this.lhs=lhs; this.rhs=rhs; }
        String desc(){
            return String.join(",", lhs) + " -> " + String.join(",", rhs);
        }
    }

    /** DC 原子：左是 tX.attr，右可为 tY.attr 或常量。 */
    static class DCAtom{
        final String lVar, lAttr;
        final String op; // "=", "==", "!=", "<", ">", "<=", ">="
        final boolean isConst;
        final String rVar, rAttr; // 若 isConst=false 使用
        final String constVal;    // 若 isConst=true 使用
        DCAtom(String lVar, String lAttr, String op, String rVar, String rAttr){
            this.lVar=lVar; this.lAttr=lAttr; this.op=op;
            this.isConst=false; this.rVar=rVar; this.rAttr=rAttr; this.constVal=null;
        }
        DCAtom(String lVar, String lAttr, String op, String constVal){
            this.lVar=lVar; this.lAttr=lAttr; this.op=op; // 修复过：this.lAttr=lattr 的拼写
            this.isConst=true; this.rVar=null; this.rAttr=null; this.constVal=constVal;
        }
    }

    /** DC 子句：原子合取 + 变量顺序。*/
    static class DCClause{
        final List<DCAtom> atoms = new ArrayList<>();
        final List<String> vars  = new ArrayList<>();
        void addVar(String v){ if(!vars.contains(v)) vars.add(v); }
    }

    /* ------------------------------
     *            CSV parser
     * ------------------------------ */

    static List<String> parseCSVLine(String line){
        List<String> out = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        for(int i=0;i<line.length();i++){
            char c = line.charAt(i);
            if(c=='"'){
                if(inQuotes && i+1<line.length() && line.charAt(i+1)=='"'){
                    sb.append('"'); i++; // 转义的双引号
                }else{
                    inQuotes = !inQuotes; // 切换状态
                }
            }else if(c==',' && !inQuotes){
                out.add(sb.toString()); sb.setLength(0);
            }else{
                sb.append(c);
            }
        }
        out.add(sb.toString());
        return out;
    }

    static String[] parseCSVToArray(String line){
        List<String> f = parseCSVLine(line);
        return f.toArray(new String[0]);
    }

    /* ------------------------------
     *            Parsing
     * ------------------------------ */

    static List<Fact> readFacts(Path csv) throws IOException{
        List<Fact> list=new ArrayList<>();
        try(BufferedReader br=Files.newBufferedReader(csv)){
            String header=br.readLine();
            if(header==null) return list;
            String[] h = parseCSVToArray(header);
            String line;
            while((line=br.readLine())!=null){
                String[] vals = parseCSVToArray(line);
                list.add(new Fact(h, vals));
            }
        }
        return list;
    }

    static List<FD> readFD(Path fd) throws IOException{
        List<FD> list=new ArrayList<>();
        if(!Files.exists(fd)) return list;
        try(BufferedReader br=Files.newBufferedReader(fd)){
            String l;
            while((l=br.readLine())!=null){
                l=l.trim();
                if(l.isEmpty()||l.startsWith("#")) continue;
                // 兼容 Unicode 箭头
                l = l.replace("→", "->");
                String[] parts=l.split("->");
                if(parts.length!=2) continue;
                List<String> lhs = new ArrayList<>();
                for(String a: parts[0].split(",")) if(!a.trim().isEmpty()) lhs.add(a.trim());
                List<String> rhs = new ArrayList<>();
                for(String b: parts[1].split(",")) if(!b.trim().isEmpty()) rhs.add(b.trim());
                if(!lhs.isEmpty() && !rhs.isEmpty())
                    list.add(new FD(lhs, rhs));
            }
        }
        return list;
    }

    static List<DCClause> readDC(Path dc) throws IOException{
        List<DCClause> dcs=new ArrayList<>();
        if(!Files.exists(dc)) return dcs;

        final Pattern ATOM = Pattern.compile(
                "\\s*(t\\d+)\\.([A-Za-z0-9_ ()%-]+)\\s*(==|!=|<=|>=|=|<|>)\\s*(?:(t\\d+)\\.([A-Za-z0-9_ ()%-]+)|([+-]?\\d+(?:\\.\\d+)?)|\"([^\"]*)\")\\s*"
        );

        try(BufferedReader br=Files.newBufferedReader(dc)){
            String l;
            while((l=br.readLine())!=null){
                l=l.trim();
                if(l.isEmpty()||l.startsWith("#")) continue;

                if(l.startsWith("¬(") && l.endsWith(")")) l = l.substring(2, l.length()-1).trim();
                if(l.startsWith("(") && l.endsWith(")"))   l = l.substring(1, l.length()-1).trim();

                String[] atoms = l.split("&&");
                DCClause clause = new DCClause();

                for(String at: atoms){
                    Matcher m = ATOM.matcher(at);
                    if(!m.matches()) continue;
                    String lVar = m.group(1);
                    String lAttr= m.group(2).trim();
                    String op   = m.group(3);

                    if(m.group(4)!=null){ // tM.attr
                        String rVar = m.group(4);
                        String rAttr= m.group(5).trim();
                        clause.atoms.add(new DCAtom(lVar, lAttr, op, rVar, rAttr));
                        clause.addVar(lVar); clause.addVar(rVar);
                    }else if(m.group(6)!=null){ // number const
                        clause.atoms.add(new DCAtom(lVar, lAttr, op, m.group(6)));
                        clause.addVar(lVar);
                    }else if(m.group(7)!=null){ // string const
                        clause.atoms.add(new DCAtom(lVar, lAttr, op, m.group(7)));
                        clause.addVar(lVar);
                    }
                }
                if(!clause.atoms.isEmpty()) dcs.add(clause);
            }
        }
        return dcs;
    }

    /** 选择型 query：每行 Attr=val1,val2,...；返回命中的 0-based 行号集合。 */
    static Set<Integer> queryIdx(List<Fact> facts,Path q) throws IOException{
        Set<Integer> idx=new HashSet<>();
        if(!Files.exists(q)) return idx;
        Map<String,Set<String>> cond=new HashMap<>();
        try(BufferedReader br=Files.newBufferedReader(q)){
            String l;
            while((l=br.readLine())!=null){
                l=l.trim();
                if(l.isEmpty()||l.startsWith("#")) continue;
                if(!l.contains("=")) continue;
                String[] p=l.split("=");
                String key=p[0].trim();
                Set<String> vals=new HashSet<>();
                for(String v: p[1].split(",")) vals.add(v.trim());
                cond.put(key, vals);
            }
        }
        for(int i=0;i<facts.size();i++){
            Fact f=facts.get(i); boolean ok=true;
            for(String k:cond.keySet()){
                String v=f.get(k);
                if(v==null||!cond.get(k).contains(v)){ ok=false; break; }
            }
            if(ok) idx.add(i);
        }
        return idx;
    }

    /* ------------------------------
     *          Evaluation
     * ------------------------------ */

    static int cmpNum(String a,String b){
        try{ return Double.compare(Double.parseDouble(a),Double.parseDouble(b)); }
        catch(Exception e){ return a.compareTo(b); }
    }

    static boolean cmp(String v1, String op, String v2){
        switch(op){
            case "=": case "==": return Objects.equals(v1, v2);
            case "!=": return !Objects.equals(v1, v2);
            case "<":  return cmpNum(v1, v2) < 0;
            case ">":  return cmpNum(v1, v2) > 0;
            case "<=": return cmpNum(v1, v2) <= 0;
            case ">=": return cmpNum(v1, v2) >= 0;
            default:   return false;
        }
    }

    static boolean violatesFD(FD fd, Fact f1, Fact f2){
        for(String a: fd.lhs){
            String v1=f1.get(a), v2=f2.get(a);
            if(v1 == null || !v1.equals(v2)) return false;
        }
        boolean allNonNull = true;
        boolean anyDiff = false;
        for(String b: fd.rhs){
            String r1=f1.get(b), r2=f2.get(b);
            if(r1==null || r2==null) { allNonNull=false; break; }
            if(!r1.equals(r2)) anyDiff = true;
        }
        return allNonNull && anyDiff;
    }

    static boolean atomSat(DCAtom a, Map<String,Fact> asg){
        Fact lf = asg.get(a.lVar);
        if(lf==null) return false;
        String v1 = lf.get(a.lAttr);
        if(v1==null) return false;
        if(a.isConst){
            return cmp(v1, a.op, a.constVal);
        }else{
            Fact rf = asg.get(a.rVar);
            if(rf==null) return false;
            String v2 = rf.get(a.rAttr);
            if(v2==null) return false;
            return cmp(v1, a.op, v2);
        }
    }

    static boolean clauseViolated(DCClause c, Map<String,Fact> asg){
        for(DCAtom a: c.atoms) if(!atomSat(a, asg)) return false;
        return true;
    }

    /* ------------------------------
     *     Hyperedges & Graph I/O
     * ------------------------------ */

    static class BuildResult {
        List<int[]> hyperedges = new ArrayList<>();
    }

    static BuildResult buildConflictHyperedges(List<Fact> facts, List<FD> fds, List<DCClause> dcs, Set<Integer> filter){
        List<Integer> idMap = (filter==null)? null : new ArrayList<>(new TreeSet<>(filter));
        List<Fact> tg       = (filter==null)? facts : new ArrayList<>();
        if(filter!=null) for(int id: idMap) tg.add(facts.get(id));

        BuildResult res = new BuildResult();
        Set<String> deDup  = new HashSet<>();

        for(int i=0;i<tg.size();i++){
            for(int j=i+1;j<tg.size();j++){
                Fact f1=tg.get(i), f2=tg.get(j);

                int id1 = (filter==null)? i+1 : idMap.get(i)+1;
                int id2 = (filter==null)? j+1 : idMap.get(j)+1;
                int a = Math.min(id1,id2), b = Math.max(id1,id2);

                for(FD fd: fds){
                    if(violatesFD(fd, f1, f2)){
                        String key = a+"-"+b;
                        if(deDup.add(key)) res.hyperedges.add(new int[]{a, b});
                    }
                }
            }
        }

        for(DCClause c: dcs){
            int k = c.vars.size();
            int n = tg.size();
            if(k==0) continue;
            int[] comb = new int[k];
            Arrays.fill(comb, -1);

            buildComb(0, 0, k, n, comb, ()->{
                Map<String,Fact> asg = new HashMap<>();
                for(int t=0;t<k;t++){
                    asg.put(c.vars.get(t), tg.get(comb[t]));
                }
                if(clauseViolated(c, asg)){
                    int[] ids = new int[k];
                    for(int t=0;t<k;t++){
                        int local = comb[t];
                        int gid = (filter==null)? (local+1) : (idMap.get(local)+1);
                        ids[t] = gid;
                    }
                    Arrays.sort(ids);
                    String key = Arrays.toString(ids);
                    if(deDup.add(key)) res.hyperedges.add(ids);
                }
            });
        }

        return res;
    }

    interface CombHit { void hit(); }
    static void buildComb(int pos, int start, int k, int n, int[] comb, CombHit cb){
        if(pos==k){ cb.hit(); return; }
        for(int i=start;i<=n-(k-pos);i++){
            comb[pos]=i;
            buildComb(pos+1, i+1, k, n, comb, cb);
        }
    }

    static Set<Long> cliqueExpandToEdges(List<int[]> hyperedges){
        Set<Long> E = new HashSet<>();
        for(int[] he: hyperedges){
            if(he.length<2) continue;
            for(int i=0;i<he.length;i++){
                for(int j=i+1;j<he.length;j++){
                    int a=he[i], b=he[j];
                    if(a==b) continue;
                    long key = (a<b)? (((long)a<<32) | (long)b) : (((long)b<<32) | (long)a);
                    E.add(key);
                }
            }
        }
        return E;
    }

    static Set<Integer> nodesFromEdges(Set<Long> E){
        Set<Integer> V = new HashSet<>();
        for(long e: E){
            int a=(int)(e>>>32), b=(int)(e & 0xffffffffL);
            V.add(a); V.add(b);
        }
        return V;
    }

    static Set<Integer> singletonNodes(List<int[]> hyperedges){
        Set<Integer> S = new HashSet<>();
        for(int[] he: hyperedges){
            if(he.length==1) S.add(he[0]);
        }
        return S;
    }

    /** Prop.10：仅保留与任一解节点连通的边；返回可达节点集。 */
    static Set<Integer> keepOnlySolutionConnected(Set<Long> E, Set<Integer> solutionNodes){
        Set<Integer> keep = new HashSet<>();
        if(solutionNodes.isEmpty()){ E.clear(); return keep; }

        Map<Integer,List<Integer>> g = new HashMap<>();
        for(long e: E){
            int a=(int)(e>>>32), b=(int)(e & 0xffffffffL);
            g.computeIfAbsent(a, k->new ArrayList<>()).add(b);
            g.computeIfAbsent(b, k->new ArrayList<>()).add(a);
        }
        Deque<Integer> dq = new ArrayDeque<>(solutionNodes);
        keep.addAll(solutionNodes);
        while(!dq.isEmpty()){
            int u = dq.pollFirst();
            for(int v: g.getOrDefault(u, Collections.emptyList())){
                if(keep.add(v)) dq.addLast(v);
            }
        }
        E.removeIf(e -> {
            int a=(int)(e>>>32), b=(int)(e & 0xffffffffL);
            return !(keep.contains(a) && keep.contains(b));
        });
        return keep;
    }

    /**
     * 写 .gr（保持原始行号）：
     *  - 头：p tw n m，其中 n=当前出现的最大原始行号（1..n 之间缺的当孤立点），m=边数
     *  - 边：直接写原始行号
     */
    static void writeGrUsingOriginalIds(Set<Long> E, Set<Integer> nodes, Path out) throws IOException{
        Files.createDirectories(out.getParent());
        if(nodes.isEmpty()){
            try(BufferedWriter bw=Files.newBufferedWriter(out)){
                bw.write("p tw 0 0"); bw.newLine();
            }
            return;
        }
        int n = Collections.max(nodes);
        List<int[]> edges = new ArrayList<>();
        for(long e: E){
            int a=(int)(e>>>32), b=(int)(e & 0xffffffffL);
            edges.add(new int[]{a, b});
        }
        try(BufferedWriter bw=Files.newBufferedWriter(out)){
            bw.write("p tw " + n + " " + edges.size()); bw.newLine();
            for(int[] ed: edges){
                bw.write(ed[0] + " " + ed[1]); bw.newLine();
            }
        }
    }

    /** 新增：把节点总数写到一个旁边的文件里（同名 + _vertex_count.txt） */
    static void writeVertexCount(Path grOut, Set<Integer> nodes) throws IOException{
        Path f = Path.of(grOut.toString().replace(".gr", "_vertex_count.txt"));
        Files.createDirectories(f.getParent());
        try(BufferedWriter bw = Files.newBufferedWriter(f)){
            bw.write(String.valueOf(nodes.size())); // 只写一个数字（顶点总数）
            bw.newLine();
        }
    }

    /** （保留旧实现但不再调用）写节点映射 */
    static void writeNodeMap(Path grOut, Set<Integer> nodes, List<Fact> facts) throws IOException{
        Path mapCsv = Path.of(grOut.toString().replace(".gr", "_node_map.csv"));
        List<Integer> order = new ArrayList<>(nodes);
        Collections.sort(order);
        try(BufferedWriter bw = Files.newBufferedWriter(mapCsv)){
            bw.write("orig_id,name,director,year,genre,released,writer,star,company"); bw.newLine();
            for(int origId : order){
                int idx = origId-1;
                if(idx<0 || idx>=facts.size()) continue;
                Fact f = facts.get(idx);
                bw.write(origId + ","
                        + csvSafe(f.get("name")) + ","
                        + csvSafe(f.get("director")) + ","
                        + csvSafe(f.get("year")) + ","
                        + csvSafe(f.get("genre")) + ","
                        + csvSafe(f.get("released")) + ","
                        + csvSafe(f.get("writer")) + ","
                        + csvSafe(f.get("star")) + ","
                        + csvSafe(f.get("company"))
                ); bw.newLine();
            }
        }
    }

    static String safe(String s){ return s==null? "NULL" : s; }
    static String csvSafe(String s){
        if(s==null) return "";
        if(s.contains(",") || s.contains("\"") || s.contains("\n")){
            return "\"" + s.replace("\"","\"\"") + "\"";
        }
        return s;
    }

    static int readTw(Path td) throws IOException{
        int max=0;
        try(BufferedReader br=Files.newBufferedReader(td)){
            String l;
            while((l=br.readLine())!=null){
                if(l.startsWith("b ")){
                    max = Math.max(max, l.trim().split(" ").length - 2);
                }
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

    /** 额外新增：写一个综合统计文件（两个图的顶点数） */
    static void writeGraphsVertexCountsSummary(String base, int conflictCount, int solutionConflictCount) throws IOException{
        Path f = Path.of(OUT_DIR, base + "_vertex_counts.txt");
        Files.createDirectories(f.getParent());
        try(BufferedWriter bw = Files.newBufferedWriter(f)){
            bw.write("conflict_graph_vertices=" + conflictCount); bw.newLine();
            bw.write("solution_conflict_graph_vertices=" + solutionConflictCount); bw.newLine();
        }
    }

    /* ------------------------------
     *               Main
     * ------------------------------ */

    public static void main(String[] args) throws Exception{

        Files.createDirectories(Paths.get(OUT_DIR));
        File[] csvFiles = new File(CSV_DIR)
                .listFiles((_, n)->n.toLowerCase().endsWith(".csv"));
        if(csvFiles==null||csvFiles.length==0){
            System.err.println("No CSV in " + CSV_DIR);
            return;
        }

        for(File csv : csvFiles){
            String base = csv.getName().replace(".csv","");
            List<Fact> facts = readFacts(csv.toPath());
            List<FD>   fds   = readFD(Path.of(FD_DIR, base + ".fd"));
            List<DCClause> dcs = readDC(Path.of(DC_DIR, base + ".dc"));

            System.out.println("[INFO] CSV=" + base + " facts=" + facts.size()
                    + " FDs=" + fds.size() + " DCs=" + dcs.size());

            // 1) 冲突超边（全局）
            BuildResult br = buildConflictHyperedges(facts, fds, dcs, null);
            List<int[]> conflictHypers = br.hyperedges;

            Set<Long> conflictEdges = cliqueExpandToEdges(conflictHypers);
            Set<Integer> conflictNodes = nodesFromEdges(conflictEdges);
            conflictNodes.addAll(singletonNodes(conflictHypers));

            // 写冲突图 + 节点数（不再写 edge_reasons）
            Path cg = Path.of(OUT_DIR, base + "_conflict_graph.gr");
            writeGrUsingOriginalIds(conflictEdges, conflictNodes, cg);
            writeVertexCount(cg, conflictNodes);

            // 2) 在冲突图上跑 Treewidth（空图不跑）
            Path td = Path.of(OUT_DIR, base + "_result.td");
            Path tw = Path.of(OUT_DIR, base + "_treewidth.txt");
            if(conflictNodes.isEmpty()){
                try(BufferedWriter bw=Files.newBufferedWriter(td)){
                    bw.write("c empty graph\ns td 0\n"); bw.newLine();
                }
                writeTw(0, tw);
            } else {
                ExactTW.main(new String[]{cg.toString(), td.toString(), "-acsd"});
                writeTw(readTw(td), tw);
            }

            // 3) 解超边（选择型查询 → 单点解）
            Set<Integer> qIdx0 = queryIdx(facts, Path.of(QUERY_DIR, base + ".query"));
            List<int[]> solutionHypers = new ArrayList<>();
            Set<Integer> solutionNodes = new HashSet<>();
            for(int id: qIdx0){
                int gid = id + 1; // 原始编号
                solutionHypers.add(new int[]{gid});
                solutionNodes.add(gid);
            }

            // 4) 合并并按 Prop.10 保留与解连通的部分
            List<int[]> unionHypers = new ArrayList<>();
            unionHypers.addAll(conflictHypers);
            unionHypers.addAll(solutionHypers);
            Set<Long> unionEdges = cliqueExpandToEdges(unionHypers);
            Set<Integer> reachable = keepOnlySolutionConnected(unionEdges, solutionNodes);

            Set<Integer> solGraphNodes = nodesFromEdges(unionEdges);
            solGraphNodes.addAll(reachable);
            solGraphNodes.addAll(solutionNodes);

            // 写解-冲突图 + 节点数
            Path sg = Path.of(OUT_DIR, base + "_solution_conflict_graph.gr");
            writeGrUsingOriginalIds(unionEdges, solGraphNodes, sg);
            writeVertexCount(sg, solGraphNodes);

            // 5) 在解-冲突图上跑 Treewidth（空图不跑）
            Path std = Path.of(OUT_DIR, base + "_solution_result.td");
            Path stw = Path.of(OUT_DIR, base + "_solution_treewidth.txt");
            if(solGraphNodes.isEmpty()){
                try(BufferedWriter bw=Files.newBufferedWriter(std)){
                    bw.write("c empty graph\ns td 0\n"); bw.newLine();
                }
                writeTw(0, stw);
            } else {
                ExactTW.main(new String[]{sg.toString(), std.toString(), "-acsd"});
                writeTw(readTw(std), stw);
            }

            // 6) 额外新增：写一个汇总 txt（两个图的顶点数）
            writeGraphsVertexCountsSummary(base, conflictNodes.size(), solGraphNodes.size());
        }
    }
}
















