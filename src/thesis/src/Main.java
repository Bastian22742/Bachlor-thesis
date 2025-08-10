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

        @Override
        public String toString() {
            return map.toString();
        }
    }

    /* ---------- DC Atom ---------- */
    static class DCAtom{
        String lTuple, lAttr, op, rTuple, rAttr, constVal;
        boolean isConst;

        // t1.A op t2.B 或 t1.A op t1.B
        DCAtom(String lTup, String lAtt, String o, String rTup, String rAtt){
            lTuple=lTup; lAttr=lAtt; op=o; rTuple=rTup; rAttr=rAtt;
            isConst=false;
        }
        // t1.A op CONST
        DCAtom(String lTup, String lAtt, String o, String constant){
            lTuple=lTup; lAttr=lAtt; op=o; constVal=constant;
            isConst=true;
        }

        @Override
        public String toString() {
            if(isConst) {
                return lTuple + "." + lAttr + " " + op + " \"" + constVal + "\"";
            } else {
                return lTuple + "." + lAttr + " " + op + " " + rTuple + "." + rAttr;
            }
        }
    }

    /* ---------- Query Atom for GGM ---------- */
    static class QueryAtomGGM {
        int atomIndex;  // q_i的索引
        Map<String, String> conditions; // 具体的属性值条件
        List<String> terms; // 完整的项列表，对应查询中的每个位置

        QueryAtomGGM(int atomIndex, Map<String, String> conditions, List<String> terms) {
            this.atomIndex = atomIndex;
            this.conditions = conditions;
            this.terms = terms;
        }

        // 获取所有变量
        Set<String> getVariables() {
            Set<String> vars = new HashSet<>();
            for(String term : terms) {
                if(isVariable(term)) {
                    vars.add(term);
                }
            }
            return vars;
        }

        boolean matchesFact(Fact fact, String[] headers) {
            // 首先检查固定条件
            for(String attr : conditions.keySet()) {
                String requiredValue = conditions.get(attr);
                String actualValue = fact.get(attr);
                if(actualValue == null || !actualValue.equals(requiredValue)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            return "q" + atomIndex + conditions.toString() + " terms:" + terms;
        }
    }

    /* ---------- Variable Constraint ---------- */
    static class VariableConstraint {
        String leftVar, operator, rightVar;

        VariableConstraint(String leftVar, String operator, String rightVar) {
            this.leftVar = leftVar;
            this.operator = operator;
            this.rightVar = rightVar;
        }

        @Override
        public String toString() {
            return leftVar + " " + operator + " " + rightVar;
        }

        boolean isSatisfied(Map<String, String> variableBindings) {
            String leftValue = variableBindings.get(leftVar);
            String rightValue = variableBindings.get(rightVar);

            if (leftValue == null || rightValue == null) {
                return false;
            }

            switch(operator) {
                case "=": return leftValue.equals(rightValue);
                case "!=": return !leftValue.equals(rightValue);
                case ">": return cmpNum(leftValue, rightValue) > 0;
                case "<": return cmpNum(leftValue, rightValue) < 0;
                case ">=": return cmpNum(leftValue, rightValue) >= 0;
                case "<=": return cmpNum(leftValue, rightValue) <= 0;
                default: return false;
            }
        }
    }

    /* ---------- 解析后的查询结构 ---------- */
    static class ParsedQuery {
        List<QueryAtomGGM> queryAtoms;
        List<VariableConstraint> variableConstraints;

        ParsedQuery(List<QueryAtomGGM> queryAtoms, List<VariableConstraint> variableConstraints) {
            this.queryAtoms = queryAtoms;
            this.variableConstraints = variableConstraints != null ? variableConstraints : new ArrayList<>();
        }

        @Override
        public String toString() {
            return "Query atoms: " + queryAtoms + ", Constraints: " + variableConstraints;
        }
    }

    /* ---------- CSV数据结构 ---------- */
    static class CsvData {
        List<Fact> facts;
        String[] headers;

        CsvData(List<Fact> facts, String[] headers) {
            this.facts = facts;
            this.headers = headers;
        }
    }

    static CsvData readFactsWithHeaders(Path csv) throws IOException{
        List<Fact> list = new ArrayList<>();
        String[] headers = null;

        try(BufferedReader br = Files.newBufferedReader(csv)){
            String headerLine = br.readLine();
            if(headerLine == null) return new CsvData(list, new String[0]);

            String[] h = headerLine.split(",");
            // 去掉列名中的引号
            for(int i = 0; i < h.length; i++) {
                h[i] = h[i].trim();
                if(h[i].startsWith("\"") && h[i].endsWith("\"")) {
                    h[i] = h[i].substring(1, h[i].length()-1);
                }
            }
            headers = h;

            System.out.println("CSV Headers after cleaning (" + h.length + "):");
            for(int i = 0; i < h.length; i++) {
                System.out.println("  [" + i + "]: '" + h[i] + "'");
            }

            String line;
            int rowCount = 0;
            while((line = br.readLine()) != null) {
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
        return new CsvData(list, headers);
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

        Pattern pat = Pattern.compile(
                "(t[12])\\.((?:\"[^\"]*\"|'[^']*'|[^\\s!=><&]+(?:\\s+[^\\s!=><&]+)*))\\s*([!=><]+)\\s*" +
                        "(?:(t[12])\\.((?:\"[^\"]*\"|'[^']*'|[^\\s!=><&]+(?:\\s+[^\\s!=><&]+)*))|" +
                        "([0-9.-]+)|" +
                        "\"([^\"]*)\"|" +
                        "'([^']*)')"
        );

        try(BufferedReader br=Files.newBufferedReader(dc)){
            String l;
            while((l=br.readLine())!=null){
                String original = l.trim();
                if(original.isEmpty()||original.startsWith("#")) continue;

                String processed = original;
                if(processed.startsWith("¬(") && processed.endsWith(")")) {
                    processed = processed.substring(2, processed.length()-1);
                }

                List<DCAtom> clause=new ArrayList<>();
                String[] atoms = processed.split("&&");

                for(String atomStr : atoms) {
                    atomStr = atomStr.trim();
                    Matcher m = pat.matcher(atomStr);

                    if(m.find()) {
                        String leftTuple = m.group(1);
                        String leftAttr = cleanAttributeName(m.group(2));
                        String op = m.group(3);

                        if(m.group(4) != null) {
                            String rightTuple = m.group(4);
                            String rightAttr = cleanAttributeName(m.group(5));
                            clause.add(new DCAtom(leftTuple, leftAttr, op, rightTuple, rightAttr));
                        }
                        else if(m.group(6) != null) {
                            String constVal = m.group(6);
                            clause.add(new DCAtom(leftTuple, leftAttr, op, constVal));
                        }
                        else if(m.group(7) != null) {
                            String constVal = m.group(7);
                            clause.add(new DCAtom(leftTuple, leftAttr, op, constVal));
                        }
                        else if(m.group(8) != null) {
                            String constVal = m.group(8);
                            clause.add(new DCAtom(leftTuple, leftAttr, op, constVal));
                        }
                    } else {
                        System.out.println("  WARNING: Could not parse atom: " + atomStr);
                    }
                }

                if(!clause.isEmpty()) {
                    dcs.add(clause);
                    System.out.println("  Added DC with " + clause.size() + " atoms");
                }
            }
        }
        System.out.println("Total DCs loaded: " + dcs.size());
        return dcs;
    }

    static String cleanAttributeName(String attr) {
        if (attr == null) return null;
        attr = attr.trim();
        if ((attr.startsWith("\"") && attr.endsWith("\"")) ||
                (attr.startsWith("'") && attr.endsWith("'"))) {
            attr = attr.substring(1, attr.length() - 1);
        }
        return attr.trim();
    }

    static int cmpNum(String a, String b){
        try{
            double d1 = Double.parseDouble(a);
            double d2 = Double.parseDouble(b);
            return Double.compare(d1, d2);
        } catch(Exception e){
            return a.compareTo(b);
        }
    }

    static boolean sat(DCAtom a, Fact f1, Fact f2){
        Fact leftFact = a.lTuple.equals("t1") ? f1 : f2;
        String v1 = leftFact.get(a.lAttr);

        if(v1 == null) {
            return false;
        }

        if(a.isConst){
            String c = a.constVal;
            switch(a.op){
                case "=": return v1.equals(c);
                case "!=": return !v1.equals(c);
                case ">": return cmpNum(v1,c)>0;
                case "<": return cmpNum(v1,c)<0;
                case ">=": return cmpNum(v1,c)>=0;
                case "<=": return cmpNum(v1,c)<=0;
                default: return false;
            }
        }else{
            Fact rightFact = a.rTuple.equals("t1") ? f1 : f2;
            String v2 = rightFact.get(a.rAttr);

            if(v2 == null) {
                return false;
            }

            switch(a.op){
                case "=": return v1.equals(v2);
                case "!=": return !v1.equals(v2);
                case ">": return cmpNum(v1,v2)>0;
                case "<": return cmpNum(v1,v2)<0;
                case ">=": return cmpNum(v1,v2)>=0;
                case "<=": return cmpNum(v1,v2)<=0;
                default: return false;
            }
        }
    }

    static boolean violatesDC(List<DCAtom> clause, Fact f1, Fact f2, int idx1, int idx2){
        // DC违反：当所有原子都为真时，DC被违反
        for(DCAtom a : clause) {
            if(!sat(a, f1, f2)) {
                return false;
            }
        }
        return true;
    }

    /* ---------- 查询解析 (新的GGM版本) ---------- */
    static ParsedQuery parseQueryForGGM(Path queryPath, String[] headers) throws IOException {
        if(!Files.exists(queryPath)) {
            System.out.println("Query file does not exist: " + queryPath);
            return new ParsedQuery(new ArrayList<>(), new ArrayList<>());
        }

        try(BufferedReader br = Files.newBufferedReader(queryPath)) {
            StringBuilder queryBuilder = new StringBuilder();
            String line;

            while((line = br.readLine()) != null) {
                line = line.trim();
                if(!line.isEmpty() && !line.startsWith("#")) {
                    queryBuilder.append(line).append(" ");
                }
            }

            String query = queryBuilder.toString().trim();
            if(query.isEmpty()) {
                return new ParsedQuery(new ArrayList<>(), new ArrayList<>());
            }

            System.out.println("Parsing query for GGM: " + query);
            return parseRelationalQueryForGGM(query, headers);
        }
    }

    static ParsedQuery parseRelationalQueryForGGM(String query, String[] headers) {
        List<QueryAtomGGM> queryAtoms = new ArrayList<>();
        List<VariableConstraint> variableConstraints = new ArrayList<>();

        // 解析关系原子 R(...) 和 T(...)
        Pattern relationPattern = Pattern.compile("([RT])\\(([^)]+)\\)");
        Matcher matcher = relationPattern.matcher(query);

        int atomIndex = 0;
        while(matcher.find()) {
            String relationName = matcher.group(1);
            String params = matcher.group(2);
            String[] values = params.split(",");

            Map<String, String> conditions = new HashMap<>();
            List<String> terms = new ArrayList<>();

            for(int i = 0; i < values.length; i++) {
                String value = cleanValue(values[i].trim());
                terms.add(value); // 保存完整的项列表

                if(!isVariable(value) && !isWildcard(value) && !value.isEmpty() && i < headers.length) {
                    conditions.put(headers[i], value);
                }
            }

            QueryAtomGGM queryAtom = new QueryAtomGGM(atomIndex, conditions, terms);
            queryAtoms.add(queryAtom);
            System.out.println("  Parsed query atom " + atomIndex + ": " + queryAtom);
            atomIndex++;
        }

        // 解析变量约束 (x1 != y1), (x1 = y1) 等
        Pattern constraintPattern = Pattern.compile("\\(\\s*(\\w+)\\s*([!=<>]+)\\s*(\\w+)\\s*\\)");
        Matcher constraintMatcher = constraintPattern.matcher(query);

        while(constraintMatcher.find()) {
            String leftVar = constraintMatcher.group(1);
            String operator = constraintMatcher.group(2);
            String rightVar = constraintMatcher.group(3);

            variableConstraints.add(new VariableConstraint(leftVar, operator, rightVar));
            System.out.println("  Parsed variable constraint: " + leftVar + " " + operator + " " + rightVar);
        }

        return new ParsedQuery(queryAtoms, variableConstraints);
    }

    static boolean isWildcard(String value) {
        if (value == null || value.isEmpty()) return false;
        return value.equals("_") || value.equals("*");
    }

    // 修改：isVariable函数现在排除通配符
    static boolean isVariable(String value) {
        if (value == null || value.isEmpty()) return false;
        // "_" 和 "*" 是通配符，不作为需绑定的变量
        if (isWildcard(value)) return false;
        return value.matches("^[a-z][0-9]*$");
    }


    static String cleanValue(String value) {
        if(value == null) return null;
        value = value.trim();
        if((value.startsWith("\"") && value.endsWith("\"")) ||
                (value.startsWith("'") && value.endsWith("'"))) {
            value = value.substring(1, value.length() - 1);
        }
        return value;
    }

    /* ---------- GGM Join边计算 ---------- */

    // 检查两个查询原子是否q-linked (修正版本)
    static boolean areQueryAtomsLinked(QueryAtomGGM atom1, QueryAtomGGM atom2,
                                       List<VariableConstraint> variableConstraints) {
        System.out.println("    Checking if q" + atom1.atomIndex + " and q" + atom2.atomIndex + " are q-linked:");

        // 条件1: 相同的原子索引
        if(atom1.atomIndex == atom2.atomIndex) {
            System.out.println("      Same atom index - q-linked");
            return true;
        }

        // 条件2: 变量有交集 (修正：应该是不为空的交集)
        Set<String> vars1 = atom1.getVariables();
        Set<String> vars2 = atom2.getVariables();
        Set<String> intersection = new HashSet<>(vars1);
        intersection.retainAll(vars2);

        System.out.println("      vars1: " + vars1);
        System.out.println("      vars2: " + vars2);
        System.out.println("      intersection: " + intersection);

        if(!intersection.isEmpty()) {
            System.out.println("      Variables have non-empty intersection - q-linked");
            return true;
        }

        // 条件3: 通过不等式约束连接
        for(VariableConstraint constraint : variableConstraints) {
            boolean hasVar1InAtom1 = vars1.contains(constraint.leftVar) || vars1.contains(constraint.rightVar);
            boolean hasVar2InAtom2 = vars2.contains(constraint.leftVar) || vars2.contains(constraint.rightVar);

            if(hasVar1InAtom1 && hasVar2InAtom2) {
                System.out.println("      Connected by constraint " + constraint + " - q-linked");
                return true;
            }
        }

        System.out.println("      Not q-linked");
        return false;
    }

    // 检查两个事实是否q-consistent (修正版本)
    static boolean areFactsQConsistent(Fact fact1, Fact fact2, QueryAtomGGM atom1, QueryAtomGGM atom2,
                                       List<VariableConstraint> variableConstraints, String[] headers) {

        System.out.println("        Checking q-consistency for facts:");
        System.out.println("          fact1: " + fact1);
        System.out.println("          fact2: " + fact2);
        System.out.println("          atom1.terms: " + atom1.terms);
        System.out.println("          atom2.terms: " + atom2.terms);

        // 首先检查基本匹配
        if(!atom1.matchesFact(fact1, headers)) {
            System.out.println("          Fact1 doesn't match atom1 conditions");
            return false;
        }
        if(!atom2.matchesFact(fact2, headers)) {
            System.out.println("          Fact2 doesn't match atom2 conditions");
            return false;
        }
        System.out.println("          Both facts match their atom conditions");

        // 建立变量绑定
        Map<String, String> variableBindings = new HashMap<>();

        // 为atom1建立变量绑定
        for(int i = 0; i < atom1.terms.size() && i < headers.length; i++) {
            String term = atom1.terms.get(i);
            if(isVariable(term)) {
                String value = fact1.get(headers[i]);
                if(value != null) {
                    variableBindings.put(term, value);
                    System.out.println("          Binding " + term + " = " + value + " (from atom1, position " + i + ")");
                }
            }
        }

        // 为atom2建立变量绑定，检查冲突
        for(int i = 0; i < atom2.terms.size() && i < headers.length; i++) {
            String term = atom2.terms.get(i);
            if(isVariable(term)) {
                String value = fact2.get(headers[i]);
                if(value != null) {
                    String existingValue = variableBindings.get(term);
                    if(existingValue != null && !existingValue.equals(value)) {
                        System.out.println("          Variable binding conflict for " + term +
                                ": " + existingValue + " vs " + value);
                        return false; // 变量绑定冲突
                    }
                    variableBindings.put(term, value);
                    System.out.println("          Binding " + term + " = " + value + " (from atom2, position " + i + ")");
                }
            }
        }

        System.out.println("          Variable bindings: " + variableBindings);

        // 检查所有相关的变量约束
        for(VariableConstraint constraint : variableConstraints) {
            // 检查约束是否与这两个原子相关
            Set<String> vars1 = atom1.getVariables();
            Set<String> vars2 = atom2.getVariables();

            boolean constraintInvolvesAtom1 = vars1.contains(constraint.leftVar) || vars1.contains(constraint.rightVar);
            boolean constraintInvolvesAtom2 = vars2.contains(constraint.leftVar) || vars2.contains(constraint.rightVar);

            if(constraintInvolvesAtom1 && constraintInvolvesAtom2) {
                System.out.println("          Checking constraint: " + constraint);
                System.out.println("            " + constraint.leftVar + " = " + variableBindings.get(constraint.leftVar));
                System.out.println("            " + constraint.rightVar + " = " + variableBindings.get(constraint.rightVar));

                if(!constraint.isSatisfied(variableBindings)) {
                    System.out.println("          Constraint not satisfied");
                    return false;
                }
                System.out.println("          Constraint satisfied");
            }
        }

        System.out.println("          Facts are q-consistent");
        return true;
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
            bw.write("Treewidth = " + tw);
            bw.newLine();
        }
    }

    /* ---------- 生成GGM图 (Gaifman Graph from MSO encoding) ---------- */
    static void writeGGMGraph(List<Fact> facts,
                              List<Map.Entry<List<String>,String>> fds,
                              List<List<DCAtom>> dcs,
                              ParsedQuery parsedQuery,
                              String[] headers,
                              Path out) throws IOException {

        System.out.println("Generating GGM (Gaifman graph from MSO encoding)...");
        System.out.println("  Facts: " + facts.size());
        System.out.println("  FDs: " + fds.size());
        System.out.println("  DCs: " + dcs.size());
        System.out.println("  Query atoms: " + parsedQuery.queryAtoms.size());
        System.out.println("  Variable constraints: " + parsedQuery.variableConstraints.size());

        Set<String> edgeSet = new HashSet<>();
        List<int[]> edges = new ArrayList<>();

        // 1. 添加冲突边 (Conflict relation - 来自FD和DC违反)
        System.out.println("  Adding Conflict edges...");
        int conflictEdges = 0;

        for(int i = 0; i < facts.size(); i++) {
            for(int j = i + 1; j < facts.size(); j++) {
                Fact f1 = facts.get(i);
                Fact f2 = facts.get(j);
                boolean hasConflict = false;

                // 检查FD违反
                for(var fd : fds) {
                    if(violatesFD(fd.getKey(), fd.getValue(), f1, f2)) {
                        hasConflict = true;
                        System.out.println("    FD conflict between facts " + i + " and " + j);
                        break;
                    }
                }

                // 检查DC违反
                if(!hasConflict) {
                    for(int dcIdx = 0; dcIdx < dcs.size(); dcIdx++) {
                        var dc = dcs.get(dcIdx);
                        if(violatesDC(dc, f1, f2, i, j)) {
                            hasConflict = true;
                            System.out.println("    DC conflict between facts " + i + " and " + j);
                            break;
                        }
                    }
                }

                if(hasConflict) {
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
        System.out.println("  Added " + conflictEdges + " Conflict edges");

        // 2. 添加Join边 (修正版本)
        System.out.println("  Adding Join edges...");
        int totalJoinEdges = 0;

        // 对于每对查询原子，如果它们是q-linked的，则添加Join边
        for(int atomI = 0; atomI < parsedQuery.queryAtoms.size(); atomI++) {
            for(int atomJ = atomI; atomJ < parsedQuery.queryAtoms.size(); atomJ++) {
                QueryAtomGGM atom1 = parsedQuery.queryAtoms.get(atomI);
                QueryAtomGGM atom2 = parsedQuery.queryAtoms.get(atomJ);

                System.out.println("  Processing Join_{" + atomI + "," + atomJ + "}:");

                // 检查这两个原子是否q-linked
                if(!areQueryAtomsLinked(atom1, atom2, parsedQuery.variableConstraints)) {
                    System.out.println("    Atoms are not q-linked, skipping");
                    continue;
                }

                System.out.println("    Atoms are q-linked, finding q-consistent fact pairs");
                int joinEdgesForThisPair = 0;

                // 找到所有匹配atom1的事实
                List<Integer> factsForAtom1 = new ArrayList<>();
                for(int factIdx = 0; factIdx < facts.size(); factIdx++) {
                    if(atom1.matchesFact(facts.get(factIdx), headers)) {
                        factsForAtom1.add(factIdx);
                    }
                }
                System.out.println("      Facts matching atom1: " + factsForAtom1);

                // 找到所有匹配atom2的事实
                List<Integer> factsForAtom2 = new ArrayList<>();
                for(int factIdx = 0; factIdx < facts.size(); factIdx++) {
                    if(atom2.matchesFact(facts.get(factIdx), headers)) {
                        factsForAtom2.add(factIdx);
                    }
                }
                System.out.println("      Facts matching atom2: " + factsForAtom2);

                // 检查所有配对，看是否q-consistent
                for(int factIdx1 : factsForAtom1) {
                    for(int factIdx2 : factsForAtom2) {
                        // 对于相同原子(atomI == atomJ)，避免自环和重复
                        if(atomI == atomJ && factIdx1 >= factIdx2) continue;

                        Fact fact1 = facts.get(factIdx1);
                        Fact fact2 = facts.get(factIdx2);

                        System.out.println("      Checking fact pair (" + factIdx1 + "," + factIdx2 + ")");
                        System.out.println("        Fact " + factIdx1 + ": " + fact1);
                        System.out.println("        Fact " + factIdx2 + ": " + fact2);

                        if(areFactsQConsistent(fact1, fact2, atom1, atom2,
                                parsedQuery.variableConstraints, headers)) {

                            int id1 = factIdx1 + 1; // 转换为1基索引
                            int id2 = factIdx2 + 1;
                            String edgeKey = Math.min(id1, id2) + "," + Math.max(id1, id2);

                            if(!edgeSet.contains(edgeKey)) {
                                edgeSet.add(edgeKey);
                                edges.add(new int[]{Math.min(id1, id2), Math.max(id1, id2)});
                                joinEdgesForThisPair++;
                                System.out.println("        Added Join edge: " + Math.min(id1, id2) + " - " + Math.max(id1, id2));
                            } else {
                                System.out.println("        Edge already exists: " + Math.min(id1, id2) + " - " + Math.max(id1, id2));
                            }
                        }
                    }
                }

                System.out.println("    Join_{" + atomI + "," + atomJ + "} added " + joinEdgesForThisPair + " edges");
                totalJoinEdges += joinEdgesForThisPair;
            }
        }

        System.out.println("  Added " + totalJoinEdges + " Join edges in total");
        System.out.println("  Total unique edges: " + edges.size());

        // 3. 写入图文件（DIMACS格式）
        Files.createDirectories(out.getParent());
        try(BufferedWriter bw = Files.newBufferedWriter(out)) {
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

        System.out.println("GGM graph written to: " + out);

        // 4. 输出图的详细信息
        outputGGMGraphStatistics(facts, parsedQuery, conflictEdges, totalJoinEdges);
    }

    // 输出GGM图的统计信息
    static void outputGGMGraphStatistics(List<Fact> facts,
                                         ParsedQuery parsedQuery,
                                         int conflictEdges, int joinEdges) {
        System.out.println("\n=== GGM (Gaifman Graph from MSO) Statistics ===");
        System.out.println("Nodes (facts): " + facts.size());
        System.out.println("Conflict edges: " + conflictEdges);
        System.out.println("Join edges: " + joinEdges);
        System.out.println("Total edges: " + (conflictEdges + joinEdges));
        System.out.println("Query atoms: " + parsedQuery.queryAtoms.size());
        System.out.println("Variable constraints: " + parsedQuery.variableConstraints.size());

        System.out.println("\nQuery atoms details:");
        for(int i = 0; i < parsedQuery.queryAtoms.size(); i++) {
            QueryAtomGGM atom = parsedQuery.queryAtoms.get(i);
            System.out.println("  q" + i + ": " + atom);
            System.out.println("    Variables: " + atom.getVariables());
        }

        System.out.println("\nVariable constraints:");
        for(VariableConstraint constraint : parsedQuery.variableConstraints) {
            System.out.println("  " + constraint);
        }

        // 计算q-linked关系
        System.out.println("\nq-linked atom pairs:");
        for(int i = 0; i < parsedQuery.queryAtoms.size(); i++) {
            for(int j = i; j < parsedQuery.queryAtoms.size(); j++) {
                QueryAtomGGM atom1 = parsedQuery.queryAtoms.get(i);
                QueryAtomGGM atom2 = parsedQuery.queryAtoms.get(j);
                if(areQueryAtomsLinked(atom1, atom2, parsedQuery.variableConstraints)) {
                    System.out.println("  q" + i + " and q" + j + " are q-linked");
                }
            }
        }
        System.out.println("===============================================\n");
    }

    /* ---------- Main方法 ---------- */
    public static void main(String[] args) throws Exception {
        Files.createDirectories(Paths.get(OUT_DIR));

        File[] csvFiles = new File(CSV_DIR).listFiles((_, name) -> name.toLowerCase().endsWith(".csv"));
        if(csvFiles == null || csvFiles.length == 0) {
            System.err.println("No CSV files found in " + CSV_DIR);
            return;
        }

        for(File csv : csvFiles) {
            String baseName = csv.getName().replace(".csv", "");
            System.out.println("\n" + "=".repeat(60));
            System.out.println("Processing: " + baseName);
            System.out.println("=".repeat(60));

            // 读取数据和表头
            CsvData csvData = readFactsWithHeaders(csv.toPath());
            List<Fact> facts = csvData.facts;
            String[] headers = csvData.headers;

            // 读取约束
            var fds = readFD(Path.of(FD_DIR, baseName + ".fd"));
            var dcs = readDC(Path.of(DC_DIR, baseName + ".dc"));

            System.out.println("Loaded constraints:");
            System.out.println("  FDs: " + fds.size());
            for(var fd : fds) {
                System.out.println("    " + fd.getKey() + " -> " + fd.getValue());
            }
            System.out.println("  DCs: " + dcs.size());
            for(int i = 0; i < dcs.size(); i++) {
                System.out.println("    DC" + (i+1) + ": " + dcs.get(i));
            }

            // 解析查询（新的GGM版本）
            ParsedQuery parsedQuery = parseQueryForGGM(Path.of(QUERY_DIR, baseName + ".q"), headers);

            if(parsedQuery.queryAtoms.isEmpty()) {
                System.out.println("WARNING: No query atoms found!");
                System.out.println("This might indicate:");
                System.out.println("  1. The query file doesn't exist or is empty");
                System.out.println("  2. The query syntax is not recognized");
                System.out.println("  3. The query format is not compatible with GGM requirements");

                // 创建空查询
                parsedQuery = new ParsedQuery(new ArrayList<>(), new ArrayList<>());
            }

            System.out.println("\nParsed query summary:");
            System.out.println("  Query atoms: " + parsedQuery.queryAtoms.size());
            System.out.println("  Variable constraints: " + parsedQuery.variableConstraints.size());

            // 生成GGM图
            Path graphPath = Path.of(OUT_DIR, baseName + "_ggm_graph.gr");
            writeGGMGraph(facts, fds, dcs, parsedQuery, headers, graphPath);

            // 计算树宽
            try {
                Path treeDecompositionPath = Path.of(OUT_DIR, baseName + "_ggm_result.td");
                ExactTW.main(new String[]{graphPath.toString(), treeDecompositionPath.toString(), "-acsd"});
                int treewidth = readTw(treeDecompositionPath);
                writeTw(treewidth, Path.of(OUT_DIR, baseName + "_ggm_treewidth.txt"));
                System.out.println("Computed GGM treewidth: " + treewidth);
            } catch(Exception e) {
                System.err.println("Error computing treewidth: " + e.getMessage());
            }

            System.out.println("Completed processing: " + baseName);
        }
        System.out.println("\nAll files processed successfully!");
    }
}