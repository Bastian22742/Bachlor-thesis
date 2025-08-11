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
                String hk = header[i]==null? null : header[i].trim();
                if(hk!=null && !hk.isEmpty() && hk.charAt(0)=='\uFEFF') hk = hk.substring(1);
                map.put(hk, v==null? null : normValue(v));
            }
        }
        String get(String k){ return map.get(k); }
    }

    /** FD: 支持多 RHS 属性。 */
    static class FD {
        final List<String> lhs;
        final List<String> rhs;
        FD(List<String> lhs, List<String> rhs){ this.lhs=lhs; this.rhs=rhs; }
        String desc(){ return String.join(",", lhs) + " -> " + String.join(",", rhs); }
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
            this.lVar=lVar; this.lAttr=lAttr; this.op=op;
            this.isConst=true; this.rVar=null; this.rAttr=null; this.constVal=constVal;
        }
    }

    /** DC 子句：原子合取 + 变量顺序。*/
    static class DCClause{
        final List<DCAtom> atoms = new ArrayList<>();
        final List<String> vars  = new ArrayList<>();
        void addVar(String v){ if(!vars.contains(v)) vars.add(v); }
    }

    /** 一个 BCQ≠：若干原子的合取（AND） */
    static class BCQ {
        final List<DCAtom> atoms = new ArrayList<>(); // 复用 DCAtom
        final List<String> vars  = new ArrayList<>();
        void addVar(String v){ if(!vars.contains(v)) vars.add(v); }
        String desc(){
            List<String> parts = new ArrayList<>();
            for(DCAtom a: atoms){
                if(a.isConst){
                    parts.add(a.lVar+"."+a.lAttr+" "+a.op+" "+a.constVal);
                }else{
                    parts.add(a.lVar+"."+a.lAttr+" "+a.op+" "+a.rVar+"."+a.rAttr);
                }
            }
            return String.join(" && ", parts);
        }
    }

    /** 一个 BUCQ≠：若干 BCQ 的析取（OR） */
    static class BUCQ {
        final List<BCQ> disj = new ArrayList<>();
        boolean isEmpty(){ return disj.isEmpty(); }
    }

    /* ------------------------------
     *        Normalization utils
     * ------------------------------ */

    /** 将输入文本统一化：去 BOM、全角→半角、奇怪空白→普通空格、常见运算符替换。 */
    static String norm(String s){
        if(s==null) return null;
        if(!s.isEmpty() && s.charAt(0)=='\uFEFF') s = s.substring(1); // strip BOM
        s = s.replace('\u00A0',' '); // NBSP -> space
        s = s.replace('（','(').replace('）',')')
                .replace('，',',').replace('。',' ')
                .replace('“','"').replace('”','"')
                .replace('‘','\'').replace('’','\'')
                .replace('＝','=').replace('＜','<').replace('＞','>')
                .replace("≤","<=").replace("≥",">=")
                .replace("＆＆","&&").replace("｜｜","||")
                .replace("→","->");
        s = s.replaceAll("\\s+"," ").trim();
        return s;
    }

    /** 轻量归一化 CSV 中的值（不动列名）。*/
    static String normValue(String s){
        if (s == null) return null;
        s = s.replace("\uFEFF","")     // BOM
                .replace('\u00A0',' ')    // NBSP
                .replace('　',' ')        // 全角空格
                .trim();
        return s;
    }

    /** 判断整串是否被一对外层括号完整包裹。*/
    static boolean isWrappedByWholeParen(String s){
        if(s==null || s.length()<2) return false;
        if(!(s.startsWith("(") && s.endsWith(")"))) return false;
        int depth = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0 && i < s.length() - 1) return false; // 中途闭合
            }
        }
        return depth == 0;
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
                    inQuotes = !inQuotes;
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
            if(!header.isEmpty() && header.charAt(0)=='\uFEFF') header = header.substring(1);
            String[] h = parseCSVToArray(header);
            String line;
            while((line=br.readLine())!=null){
                if(!line.isEmpty() && line.charAt(0)=='\uFEFF') line = line.substring(1);
                list.add(new Fact(h, parseCSVToArray(line)));
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
                l = norm(l);
                if(l.isEmpty()||l.startsWith("#")) continue;
                String[] parts=l.split("->");
                if(parts.length!=2) continue;
                List<String> lhs = new ArrayList<>();
                for(String a: parts[0].split(",")) {
                    String t = norm(a);
                    if(!t.isEmpty()) lhs.add(t);
                }
                List<String> rhs = new ArrayList<>();
                for(String b: parts[1].split(",")) {
                    String t = norm(b);
                    if(!t.isEmpty()) rhs.add(t);
                }
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
                "\\s*(t\\d+)\\.([A-Za-z0-9_ ()%-]+)\\s*(==|!=|<=|>=|=|<|>)\\s*"
                        + "(?:(t\\d+)\\.([A-Za-z0-9_ ()%-]+)|([+-]?\\d+(?:\\.\\d+)?)|\"([^\"]*)\"|'([^']*)')\\s*"
        );

        try(BufferedReader br=Files.newBufferedReader(dc)){
            String l;
            while((l=br.readLine())!=null){
                l = norm(l);
                if(l.isEmpty()||l.startsWith("#")) continue;

                // 只有当整串被外层括号包裹时才裁掉
                if(l.startsWith("¬(") && l.endsWith(")") && isWrappedByWholeParen(l.substring(1)))
                    l = l.substring(2, l.length()-1).trim();
                if(isWrappedByWholeParen(l))
                    l = l.substring(1, l.length()-1).trim();

                String[] atoms = l.split("&&");
                DCClause clause = new DCClause();

                for(String at: atoms){
                    String atn = norm(at);
                    Matcher m = ATOM.matcher(atn);
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
                    }else if(m.group(7)!=null || m.group(8)!=null){ // string const
                        String sval = (m.group(7)!=null ? m.group(7) : m.group(8));
                        clause.atoms.add(new DCAtom(lVar, lAttr, op, sval));
                        clause.addVar(lVar);
                    }
                }
                if(!clause.atoms.isEmpty()) dcs.add(clause);
            }
        }
        return dcs;
    }

    /** 解析 .query 为 BUCQ≠（若失败则返回空，主流程再决定是否回退） */
    static BUCQ readBUCQ(Path q) throws IOException {
        BUCQ res = new BUCQ();
        if(!Files.exists(q)) return res;

        final Pattern ATOM = Pattern.compile(
                "\\s*(t\\d+)\\.([A-Za-z0-9_ ()%-]+)\\s*(==|!=|<=|>=|=|<|>)\\s*"
                        + "(?:(t\\d+)\\.([A-Za-z0-9_ ()%-]+)|([+-]?\\d+(?:\\.\\d+)?)|\"([^\"]*)\"|'([^']*)')\\s*"
        );

        // 读全文件，去掉注释与空行并做 norm
        List<String> lines = Files.readAllLines(q);
        boolean hadContent = false;
        StringBuilder sb = new StringBuilder();
        for(String line : lines){
            String t = norm(line);
            if(t.startsWith("#") || t.isEmpty()) continue;
            hadContent = true;
            sb.append(t).append(" ");
        }
        String s = norm(sb.toString());
        if(s.isEmpty()){
            if(hadContent){
                System.err.println("[WARN] Query has non-empty lines but became empty after normalization.");
            }
            return res;
        }

        // 只有当“整串”被外层括号完整包裹时才裁
        if(s.startsWith("¬(") && s.endsWith(")") && isWrappedByWholeParen(s.substring(1)))
            s = s.substring(2, s.length()-1).trim();
        if(isWrappedByWholeParen(s))
            s = s.substring(1, s.length()-1).trim();

        // 顶层按 || 切 BCQ（每个分支外侧可选括号）
        String[] disj = s.split("\\|\\|");
        for(String part : disj){
            String p = norm(part);
            if(p.isEmpty()) continue;
            if(isWrappedByWholeParen(p)) p = p.substring(1, p.length()-1).trim();

            String[] atoms = p.split("&&");
            BCQ bcq = new BCQ();
            for(String a : atoms){
                String an = norm(a);
                Matcher m = ATOM.matcher(an);
                if(!m.matches()) { bcq.atoms.clear(); break; }
                String lVar = m.group(1), lAttr = m.group(2).trim(), op = m.group(3);
                if(m.group(4)!=null){ // tY.attr
                    String rVar = m.group(4), rAttr = m.group(5).trim();
                    bcq.atoms.add(new DCAtom(lVar, lAttr, op, rVar, rAttr));
                    bcq.addVar(lVar); bcq.addVar(rVar);
                }else if(m.group(6)!=null){ // number
                    bcq.atoms.add(new DCAtom(lVar, lAttr, op, m.group(6)));
                    bcq.addVar(lVar);
                }else if(m.group(7)!=null || m.group(8)!=null){ // string
                    String sval = (m.group(7)!=null ? m.group(7) : m.group(8));
                    bcq.atoms.add(new DCAtom(lVar, lAttr, op, sval));
                    bcq.addVar(lVar);
                }
            }
            if(!bcq.atoms.isEmpty()) res.disj.add(bcq);
        }

        if(hadContent && res.isEmpty()){
            System.err.println("[WARN] Query file found and non-empty, but no BCQ/BUCQ atoms were parsed after normalization. "
                    + "Check quotes/operators/full-width characters or parentheses wrapping.");
        }
        return res;
    }

    /** 选择型 query：每行 Attr=val1,val2,...；返回命中的 0-based 行号集合。 */
    static Set<Integer> queryIdx(List<Fact> facts,Path q) throws IOException{
        Set<Integer> idx=new HashSet<>();
        if(!Files.exists(q)) return idx;
        Map<String,Set<String>> cond=new HashMap<>();
        boolean hadAny = false;
        try(BufferedReader br=Files.newBufferedReader(q)){
            String l;
            while((l=br.readLine())!=null){
                l = norm(l);
                if(l.isEmpty()||l.startsWith("#")) continue;
                if(!l.contains("=")) continue;
                hadAny = true;
                String[] p=l.split("=");
                String key=p[0].trim();
                Set<String> vals=new HashSet<>();
                for(String v: p[1].split(",")) vals.add(norm(v));
                cond.put(key, vals);
            }
        }
        if(!hadAny) return idx;
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
        if(v1==null || v2==null) return false;
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
     *  冲突超边（FD/DC） & 图 I/O
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

        // FD -> 二元超边
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

        // DC -> k 元超边
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

    /** 写一个旁路的顶点计数文件 */
    static void writeVertexCount(Path grOut, Set<Integer> nodes) throws IOException{
        Path f = Path.of(grOut.toString().replace(".gr", "_vertex_count.txt"));
        Files.createDirectories(f.getParent());
        try(BufferedWriter bw = Files.newBufferedWriter(f)){
            bw.write(String.valueOf(nodes.size()));
            bw.newLine();
        }
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

    /** 汇总两个图的顶点数 */
    static void writeGraphsVertexCountsSummary(String base, int conflictCount, int solutionConflictCount) throws IOException{
        Path f = Path.of(OUT_DIR, base + "_vertex_counts.txt");
        Files.createDirectories(f.getParent());
        try(BufferedWriter bw = Files.newBufferedWriter(f)){
            bw.write("conflict_graph_vertices=" + conflictCount); bw.newLine();
            bw.write("solution_conflict_graph_vertices=" + solutionConflictCount); bw.newLine();
        }
    }

    /* ------------------------------
     *     BCQ/BUCQ → 解超边
     * ------------------------------ */

    /** 顶层：BUCQ 的所有子 BCQ 的解边并集，随后做极小化 */
    static List<int[]> buildSolutionHyperedgesBUCQ(List<Fact> facts, BUCQ bu) {
        List<int[]> out = new ArrayList<>();
        for(BCQ bcq : bu.disj){
            out.addAll(evalBCQToWitnesses(facts, bcq));
        }
        out = minimizeHyperedges(out);
        return out;
    }

    /** 对单个 BCQ 做回溯匹配，返回 witness（使用到的 1-based 原始行号集合） */
    static List<int[]> evalBCQToWitnesses(List<Fact> facts, BCQ q){
        // 预筛：每个变量根据“常量比较”的原子得到候选行
        Map<String, List<Integer>> cand = new HashMap<>();
        for(String v : q.vars){
            List<Integer> list = new ArrayList<>();
            outer: for(int i=0;i<facts.size();i++){
                Fact f = facts.get(i);
                for(DCAtom a : q.atoms){
                    if(!a.lVar.equals(v)) continue;
                    if(!a.isConst) continue; // 只用常量比较做一元筛选
                    String v1 = f.get(a.lAttr);
                    if(v1==null || !cmp(v1, a.op, a.constVal)) continue outer;
                }
                list.add(i);
            }
            if(list.isEmpty()){ // 某变量无候选，直接空
                return Collections.emptyList();
            }
            cand.put(v, list);
        }

        List<int[]> res = new ArrayList<>();
        Map<String,Integer> asg = new HashMap<>(); // var -> row index
        backtrackBCQ(0, q, facts, cand, asg, res);

        // 归一化：0-based → 1-based, 排序, 去重
        res = dedupAndNormalize(res);
        return res;
    }

    static void backtrackBCQ(int pos, BCQ q, List<Fact> facts,
                             Map<String,List<Integer>> cand,
                             Map<String,Integer> asg,
                             List<int[]> res){
        if(pos == q.vars.size()){
            if(allAtomsSat(q.atoms, asg, facts)){
                TreeSet<Integer> set = new TreeSet<>();
                for(int r : asg.values()) set.add(r);
                int[] arr = set.stream().mapToInt(x->x).toArray();
                res.add(arr);
            }
            return;
        }
        String v = q.vars.get(pos);
        for(int idx : cand.get(v)){
            if(partialOk(v, idx, q.atoms, asg, facts)){
                asg.put(v, idx);
                backtrackBCQ(pos+1, q, facts, cand, asg, res);
                asg.remove(v);
            }
        }
    }

    static boolean allAtomsSat(List<DCAtom> atoms, Map<String,Integer> asg, List<Fact> facts){
        for(DCAtom a: atoms){
            Integer li = asg.get(a.lVar);
            if(li==null) return false;
            String v1 = facts.get(li).get(a.lAttr);
            if(v1==null) return false;
            if(a.isConst){
                if(!cmp(v1, a.op, a.constVal)) return false;
            }else{
                Integer ri = asg.get(a.rVar);
                if(ri==null) return false;
                String v2 = facts.get(ri).get(a.rAttr);
                if(v2==null || !cmp(v1, a.op, v2)) return false;
            }
        }
        return true;
    }

    static boolean partialOk(String newVar, int newIdx, List<DCAtom> atoms,
                             Map<String,Integer> asg, List<Fact> facts){
        Map<String,Integer> tmp = new HashMap<>(asg);
        tmp.put(newVar, newIdx);
        for(DCAtom a: atoms){
            Integer li = tmp.get(a.lVar);
            if(li==null) continue;
            if(a.isConst){
                String v1 = facts.get(li).get(a.lAttr);
                if(v1==null || !cmp(v1, a.op, a.constVal)) return false;
            }else{
                Integer ri = tmp.get(a.rVar);
                if(ri==null) continue;
                String v1 = facts.get(li).get(a.lAttr);
                String v2 = facts.get(ri).get(a.rAttr);
                if(v1==null || v2==null || !cmp(v1, a.op, v2)) return false;
            }
        }
        return true;
    }

    static List<int[]> dedupAndNormalize(List<int[]> edges){
        Set<String> seen = new HashSet<>();
        List<int[]> out = new ArrayList<>();
        for(int[] e : edges){
            int[] x = Arrays.stream(e).map(i->i+1).sorted().toArray();
            String key = Arrays.toString(x);
            if(seen.add(key)) out.add(x);
        }
        return out;
    }

    /** 移除任何“为另一条边超集”的超边，保留极小 witness */
    static List<int[]> minimizeHyperedges(List<int[]> edges){
        List<int[]> out = new ArrayList<>(edges);
        out.sort(Comparator.comparingInt(a->a.length)); // 先短后长
        List<int[]> keep = new ArrayList<>();
        for(int[] e : out){
            boolean dominated = false;
            for(int[] s : keep){
                if(isSubset(s, e)){ dominated = true; break; }
            }
            if(!dominated) keep.add(e);
        }
        return keep;
    }

    static boolean isSubset(int[] small, int[] big){
        int i=0,j=0;
        while(i<small.length && j<big.length){
            if(small[i]==big[j]){ i++; j++; }
            else if(small[i] > big[j]) j++;
            else return false;
        }
        return i==small.length;
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

            // 写冲突图 + 顶点计数
            Path cg = Path.of(OUT_DIR, base + "_conflict_graph.gr");
            writeGrUsingOriginalIds(conflictEdges, conflictNodes, cg);
            writeVertexCount(cg, conflictNodes); // 保留冲突图的 vertex_counts

            // 2) 解析查询，得到解超边（Es）
            List<int[]> solutionHypers = new ArrayList<>();
            Set<Integer> solutionNodes = new HashSet<>();

            Path qpath = Path.of(QUERY_DIR, base + ".query");
            boolean queryFileExists = Files.exists(qpath);
            String rawQuery = null;
            if(queryFileExists){
                rawQuery = String.join("\n", Files.readAllLines(qpath));
            }

            BUCQ bu = readBUCQ(qpath);
            if(queryFileExists && !bu.isEmpty()){
                solutionHypers = buildSolutionHyperedgesBUCQ(facts, bu);
                for(int[] he : solutionHypers) for(int id : he) solutionNodes.add(id);
                System.out.println("[INFO] Query parsed as BUCQ (BCQ count=" + bu.disj.size() + "); solution hyperedges = " + solutionHypers.size());
            } else if(queryFileExists) {
                Set<Integer> qIdx0 = queryIdx(facts, qpath);
                if(!qIdx0.isEmpty()){
                    for(int id: qIdx0){
                        int gid = id + 1; // 原始编号
                        solutionHypers.add(new int[]{gid});
                        solutionNodes.add(gid);
                    }
                    System.out.println("[INFO] Query parsed as simple selection; solution nodes = " + solutionNodes.size());
                } else {
                    System.err.println("[ERROR] Query file exists but could not be parsed as BCQ/BUCQ nor as selection. "
                            + "Please check syntax. Content (normalized head): "
                            + (rawQuery==null? "<null>" : norm(rawQuery).substring(0, Math.min(200, norm(rawQuery).length()))));
                }
            } else {
                System.out.println("[INFO] No query file; skipping solution set.");
            }

            // === 新增：导出 Es 为单独的 .gr（供 DP 使用） ===
            Set<Long> solutionEdgesGraph = cliqueExpandToEdges(solutionHypers);
            Set<Integer> solutionGraphNodes = nodesFromEdges(solutionEdgesGraph);
            solutionGraphNodes.addAll(singletonNodes(solutionHypers));
            Path solGr = Path.of(OUT_DIR, base + "_solutions_graph.gr");
            writeGrUsingOriginalIds(solutionEdgesGraph, solutionGraphNodes, solGr);

            // 3) 合并并按 Prop.10 保留与解连通的部分，写解-冲突图（联合图）
            List<int[]> unionHypers = new ArrayList<>();
            unionHypers.addAll(conflictHypers);
            unionHypers.addAll(solutionHypers);
            Set<Long> unionEdges = cliqueExpandToEdges(unionHypers);
            Set<Integer> reachable = keepOnlySolutionConnected(unionEdges, solutionNodes);

            Set<Integer> solGraphNodes = nodesFromEdges(unionEdges);
            solGraphNodes.addAll(reachable);
            solGraphNodes.addAll(solutionNodes);

            Path sg = Path.of(OUT_DIR, base + "_solution_conflict_graph.gr");
            writeGrUsingOriginalIds(unionEdges, solGraphNodes, sg);
            // 不再写解-冲突图的 vertex_counts 文件

            // 4) 在解-冲突图上跑 Treewidth（得到 .td，供 DP 用；空图不跑）
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

            // 5) 调用 DP：NUMBERFALSIFY，并落盘结果
            try {
                var H  = GraphIO.loadHypergraph(
                        Path.of(OUT_DIR, base + "_conflict_graph.gr"),   // Ec
                        Path.of(OUT_DIR, base + "_solutions_graph.gr")   // Es
                );
                var TD = GraphIO.loadTreeDecomposition(
                        Path.of(OUT_DIR, base + "_solution_result.td")   // 联合图（Ec∪Es）的树分解
                );
                // 可选：第一次建议打开校验
                GraphIO.assertEdgeCoverage(H, TD);

                var eng = new Dynmaic_Programming_Based_for_CQA.Engine(H, TD);
                var falsifyCount = eng.numberFalsify();
                System.out.println("[CQA] NUMBERFALSIFY(" + base + ") = " + falsifyCount);

                Path outTxt = Path.of(OUT_DIR, base + "_cqa_numberfalsify.txt");
                Files.writeString(outTxt, falsifyCount.toString());
            } catch (Exception ex) {
                System.err.println("[CQA] counting failed for " + base + ": " + ex.getMessage());
            }

            // 6) 汇总（保持原有的汇总文件）
            writeGraphsVertexCountsSummary(base, conflictNodes.size(), solGraphNodes.size());
        }
    }
}


















