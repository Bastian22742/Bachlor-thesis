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

    /* ---------- 查询处理 ---------- */
    static class ConjunctiveQuery {
        List<QueryAtom> atoms;
        List<VariableConstraint> variableConstraints;

        ConjunctiveQuery(List<QueryAtom> atoms) {
            this.atoms = atoms;
            this.variableConstraints = new ArrayList<>();
        }

        ConjunctiveQuery(List<QueryAtom> atoms, List<VariableConstraint> variableConstraints) {
            this.atoms = atoms;
            this.variableConstraints = variableConstraints;
        }

        // 检查给定的事实集合是否满足这个合取查询
        boolean isSatisfiedBy(List<Fact> facts, Set<Integer> indices, String[] headers) {
            // 首先检查基本的原子条件
            for(QueryAtom atom : atoms) {
                if(!atom.isSatisfiedBy(facts, indices)) {
                    return false;
                }
            }

            // 然后检查变量约束
            if (!variableConstraints.isEmpty()) {
                return checkVariableConstraints(facts, indices, headers);
            }

            return true;
        }

        private boolean checkVariableConstraints(List<Fact> facts, Set<Integer> indices, String[] headers) {
            // 为每个原子找到匹配的事实，并建立变量绑定
            List<List<Integer>> atomMatches = new ArrayList<>();

            for (QueryAtom atom : atoms) {
                List<Integer> matches = new ArrayList<>();
                for (Integer idx : indices) {
                    Fact fact = facts.get(idx);
                    if (atom.isSatisfiedByFact(fact)) {
                        matches.add(idx);
                    }
                }
                atomMatches.add(matches);
            }

            // 生成所有可能的变量绑定组合
            return generateVariableBindings(atomMatches, 0, new ArrayList<>(), facts, headers);
        }

        private boolean generateVariableBindings(List<List<Integer>> atomMatches, int atomIndex,
                                                 List<Integer> currentBinding, List<Fact> facts, String[] headers) {
            if (atomIndex == atomMatches.size()) {
                // 检查当前绑定是否满足所有变量约束
                Map<String, String> variableMap = buildVariableMap(currentBinding, facts, headers);
                return checkAllVariableConstraints(variableMap);
            }

            // 尝试当前原子的所有可能匹配
            for (Integer factIndex : atomMatches.get(atomIndex)) {
                currentBinding.add(factIndex);
                if (generateVariableBindings(atomMatches, atomIndex + 1, currentBinding, facts, headers)) {
                    return true;
                }
                currentBinding.remove(currentBinding.size() - 1);
            }

            return false;
        }

        private Map<String, String> buildVariableMap(List<Integer> factIndices, List<Fact> facts, String[] headers) {
            Map<String, String> variableMap = new HashMap<>();

            for (int i = 0; i < factIndices.size(); i++) {
                Integer factIndex = factIndices.get(i);
                Fact fact = facts.get(factIndex);

                // 根据原子中的变量模式建立绑定
                for (int j = 0; j < headers.length; j++) {
                    String headerName = headers[j];
                    String value = fact.get(headerName);

                    // 为这个原子的第j个位置的变量建立绑定
                    String varName = getVariableName(i, j);
                    if (varName != null && value != null) {
                        variableMap.put(varName, value);
                    }
                }
            }

            return variableMap;
        }

        private String getVariableName(int atomIndex, int positionIndex) {
            // 根据原子索引和位置索引生成变量名
            // 简化实现：假设第一个原子使用 x1, x2, x3...，第二个使用 y1, y2, y3...
            char prefix = (atomIndex == 0) ? 'x' : 'y';
            return prefix + String.valueOf(positionIndex + 1);
        }

        private boolean checkAllVariableConstraints(Map<String, String> variableMap) {
            for (VariableConstraint constraint : variableConstraints) {
                if (!constraint.isSatisfied(variableMap)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            if (variableConstraints.isEmpty()) {
                return atoms.toString();
            } else {
                return atoms.toString() + " with constraints: " + variableConstraints.toString();
            }
        }
    }

    static class QueryAtom {
        Map<String, String> conditions;

        QueryAtom(Map<String, String> conditions) {
            this.conditions = conditions;
        }

        boolean isSatisfiedBy(List<Fact> facts, Set<Integer> indices) {
            for(Integer idx : indices) {
                Fact fact = facts.get(idx);
                if (isSatisfiedByFact(fact)) {
                    return true;
                }
            }
            return false;
        }

        boolean isSatisfiedByFact(Fact fact) {
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
            return conditions.toString();
        }
    }

    /* ---------- 查找最小解决方案集合 ---------- */
    static List<Set<Integer>> findMinimalSolutionSets(List<Fact> facts, Path queryPath, String[] headers) throws IOException {
        List<Set<Integer>> minimalSets = new ArrayList<>();
        if(!Files.exists(queryPath)) {
            System.out.println("Query file does not exist: " + queryPath);
            return minimalSets;
        }

        // 解析查询
        List<ConjunctiveQuery> queries = parseUCQ(queryPath, headers);
        if(queries.isEmpty()) {
            System.out.println("No valid queries parsed");
            return minimalSets;
        }

        System.out.println("Parsed " + queries.size() + " conjunctive queries");

        // 对于每个合取查询，找到最小满足集合
        for(int queryIdx = 0; queryIdx < queries.size(); queryIdx++) {
            ConjunctiveQuery query = queries.get(queryIdx);
            System.out.println("Finding minimal solution sets for query " + (queryIdx + 1) + ": " + query);

            List<Set<Integer>> queryMinimalSets = findMinimalSetsForQuery(facts, query, headers);

            System.out.println("  Found " + queryMinimalSets.size() + " minimal sets for this query");
            for(int i = 0; i < queryMinimalSets.size(); i++) {
                System.out.println("    Minimal set " + (i+1) + ": " + queryMinimalSets.get(i));
            }

            minimalSets.addAll(queryMinimalSets);
        }

        // 去除重复的集合
        List<Set<Integer>> uniqueMinimalSets = new ArrayList<>();
        for(Set<Integer> set : minimalSets) {
            boolean isDuplicate = false;
            for(Set<Integer> existing : uniqueMinimalSets) {
                if(existing.equals(set)) {
                    isDuplicate = true;
                    break;
                }
            }
            if(!isDuplicate) {
                uniqueMinimalSets.add(set);
            }
        }

        return uniqueMinimalSets;
    }

    // 为单个查询找到最小满足集合
    static List<Set<Integer>> findMinimalSetsForQuery(List<Fact> facts, ConjunctiveQuery query, String[] headers) {
        List<Set<Integer>> minimalSets = new ArrayList<>();
        int n = facts.size();

        // 按大小递增搜索，找到满足查询的最小集合
        for(int size = 1; size <= n; size++) {
            System.out.println("    Checking sets of size " + size + "...");
            List<Set<Integer>> currentSizeSets = new ArrayList<>();

            generateCombinations(facts, query, size, 0, new ArrayList<>(), currentSizeSets, headers);

            if(!currentSizeSets.isEmpty()) {
                System.out.println("    Found " + currentSizeSets.size() + " satisfying sets of size " + size);

                // 检查是否为最小集合（没有真子集也满足查询）
                for(Set<Integer> candidate : currentSizeSets) {
                    boolean isMinimal = true;

                    // 检查所有真子集
                    for(Set<Integer> existing : minimalSets) {
                        if(candidate.containsAll(existing)) {
                            isMinimal = false;
                            break;
                        }
                    }

                    if(isMinimal) {
                        minimalSets.add(candidate);
                    }
                }

                // 如果找到了满足条件的集合，不需要检查更大的集合
                // （因为我们要找的是最小集合）
                if(!minimalSets.isEmpty()) {
                    break;
                }
            }
        }

        return minimalSets;
    }

    static void generateCombinations(List<Fact> facts, ConjunctiveQuery query,
                                     int targetSize, int startIndex,
                                     List<Integer> current, List<Set<Integer>> results, String[] headers) {
        if(current.size() == targetSize) {
            Set<Integer> currentSet = new HashSet<>(current);
            if(query.isSatisfiedBy(facts, currentSet, headers)) {
                results.add(currentSet);
            }
            return;
        }

        for(int i = startIndex; i < facts.size(); i++) {
            current.add(i);
            generateCombinations(facts, query, targetSize, i + 1, current, results, headers);
            current.remove(current.size() - 1);
        }
    }

    /* ---------- 查询解析 ---------- */
    static List<ConjunctiveQuery> parseUCQ(Path queryPath, String[] headers) throws IOException {
        List<ConjunctiveQuery> disjuncts = new ArrayList<>();

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
            if(query.isEmpty()) return disjuncts;

            System.out.println("Parsing query: " + query);

            if(query.contains("R(") || query.contains("T(")) {
                return parseRelationalUCQ(query, headers);
            } else {
                return parseAttributeValueUCQ(query);
            }
        }
    }

    static List<ConjunctiveQuery> parseRelationalUCQ(String query, String[] headers) {
        List<ConjunctiveQuery> disjuncts = new ArrayList<>();

        String[] disjunctStrs = query.split("[∨]|\\|\\|");

        for(String disjunctStr : disjunctStrs) {
            disjunctStr = disjunctStr.trim();
            List<QueryAtom> atoms = new ArrayList<>();
            List<VariableConstraint> variableConstraints = new ArrayList<>();

            // 解析关系原子 R(...) 和 T(...)
            Pattern relationPattern = Pattern.compile("[RT]\\(([^)]+)\\)");
            Matcher matcher = relationPattern.matcher(disjunctStr);

            while(matcher.find()) {
                String params = matcher.group(1);
                String[] values = params.split(",");

                Map<String, String> conditions = new HashMap<>();

                for(int i = 0; i < values.length && i < headers.length; i++) {
                    String value = cleanValue(values[i].trim());

                    if(!isVariable(value) && !value.isEmpty()) {
                        conditions.put(headers[i], value);
                    }
                }

                if(!conditions.isEmpty()) {
                    atoms.add(new QueryAtom(conditions));
                    System.out.println("  Parsed relational atom: " + conditions);
                }
            }

            // 解析变量约束 (x1 != y1), (x1 = y1) 等
            Pattern constraintPattern = Pattern.compile("\\(\\s*(\\w+)\\s*([!=<>]+)\\s*(\\w+)\\s*\\)");
            Matcher constraintMatcher = constraintPattern.matcher(disjunctStr);

            while(constraintMatcher.find()) {
                String leftVar = constraintMatcher.group(1);
                String operator = constraintMatcher.group(2);
                String rightVar = constraintMatcher.group(3);

                variableConstraints.add(new VariableConstraint(leftVar, operator, rightVar));
                System.out.println("  Parsed variable constraint: " + leftVar + " " + operator + " " + rightVar);
            }

            if(!atoms.isEmpty()) {
                disjuncts.add(new ConjunctiveQuery(atoms, variableConstraints));
            }
        }

        return disjuncts;
    }

    static List<ConjunctiveQuery> parseAttributeValueUCQ(String query) {
        List<ConjunctiveQuery> disjuncts = new ArrayList<>();

        String[] orParts = query.split("\\|\\|");

        for(String orPart : orParts) {
            orPart = orPart.trim();

            List<Map<String, String>> alternatives = expandAlternatives(orPart);

            for(Map<String, String> conditions : alternatives) {
                if(!conditions.isEmpty()) {
                    List<QueryAtom> atoms = new ArrayList<>();
                    atoms.add(new QueryAtom(conditions));
                    disjuncts.add(new ConjunctiveQuery(atoms));
                    System.out.println("  Parsed attribute-value query: " + conditions);
                }
            }
        }

        return disjuncts;
    }

    static List<Map<String, String>> expandAlternatives(String queryPart) {
        List<Map<String, String>> result = new ArrayList<>();
        Map<String, String> baseConditions = new HashMap<>();

        Pattern bracketPattern = Pattern.compile("\\(([^)]+)\\)");
        Matcher matcher = bracketPattern.matcher(queryPart);

        List<List<Map.Entry<String, String>>> alternativeGroups = new ArrayList<>();

        String withoutBrackets = queryPart;
        while(matcher.find()) {
            String bracketContent = matcher.group(1);
            List<Map.Entry<String, String>> alternatives = new ArrayList<>();

            String[] choices = bracketContent.split("\\|\\|");
            for(String choice : choices) {
                choice = choice.trim();
                if(choice.contains("=")) {
                    String[] parts = choice.split("=", 2);
                    if(parts.length == 2) {
                        alternatives.add(new AbstractMap.SimpleEntry<>(
                                parts[0].trim(), cleanValue(parts[1].trim())
                        ));
                    }
                }
            }

            if(!alternatives.isEmpty()) {
                alternativeGroups.add(alternatives);
            }

            withoutBrackets = withoutBrackets.replace(matcher.group(0), "");
        }

        String[] baseParts = withoutBrackets.split("&&");
        for(String part : baseParts) {
            part = part.trim();
            if(part.contains("=")) {
                String[] kv = part.split("=", 2);
                if(kv.length == 2) {
                    baseConditions.put(kv[0].trim(), cleanValue(kv[1].trim()));
                }
            }
        }

        if(alternativeGroups.isEmpty()) {
            result.add(baseConditions);
        } else {
            generateCartesianProduct(baseConditions, alternativeGroups, 0,
                    new HashMap<>(), result);
        }

        return result;
    }

    static void generateCartesianProduct(Map<String, String> baseConditions,
                                         List<List<Map.Entry<String, String>>> alternativeGroups,
                                         int groupIndex,
                                         Map<String, String> current,
                                         List<Map<String, String>> result) {
        if(groupIndex == alternativeGroups.size()) {
            Map<String, String> finalConditions = new HashMap<>(baseConditions);
            finalConditions.putAll(current);
            result.add(finalConditions);
            return;
        }

        List<Map.Entry<String, String>> currentGroup = alternativeGroups.get(groupIndex);
        for(Map.Entry<String, String> choice : currentGroup) {
            Map<String, String> newCurrent = new HashMap<>(current);
            newCurrent.put(choice.getKey(), choice.getValue());
            generateCartesianProduct(baseConditions, alternativeGroups,
                    groupIndex + 1, newCurrent, result);
        }
    }

    static boolean isVariable(String value) {
        if(value == null || value.isEmpty()) return true;
        return value.equals("_") ||
                value.equals("*") ||
                value.matches("^[xy][0-9]*$") ||
                value.matches("^[A-Z][0-9]*$");
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

    /* ---------- 生成解-冲突图 ---------- */
    static void writeSolutionConflictGraph(List<Fact> facts,
                                           List<Map.Entry<List<String>,String>> fds,
                                           List<List<DCAtom>> dcs,
                                           List<Set<Integer>> minimalSolutionSets,
                                           Path out) throws IOException {

        System.out.println("Generating solution-conflict hypergraph...");
        System.out.println("  Facts: " + facts.size());
        System.out.println("  FDs: " + fds.size());
        System.out.println("  DCs: " + dcs.size());
        System.out.println("  Minimal solution sets: " + minimalSolutionSets.size());

        Set<String> edgeSet = new HashSet<>();
        List<int[]> edges = new ArrayList<>();

        // 1. 添加冲突边 (来自FD和DC违反)
        System.out.println("  Adding conflict edges...");
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
                        System.out.println("    FD conflict between facts " + i + " and " + j +
                                " (FD: " + fd.getKey() + " -> " + fd.getValue() + ")");
                        break;
                    }
                }

                // 检查DC违反
                if(!hasConflict) {
                    for(int dcIdx = 0; dcIdx < dcs.size(); dcIdx++) {
                        var dc = dcs.get(dcIdx);
                        if(violatesDC(dc, f1, f2, i, j)) {
                            hasConflict = true;
                            System.out.println("    DC conflict between facts " + i + " and " + j +
                                    " (DC " + dcIdx + ")");
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
        System.out.println("  Added " + conflictEdges + " conflict edges");

        // 2. 为解决方案超边创建辅助边
        System.out.println("  Adding solution hyperedges as cliques...");
        int solutionEdges = 0;

        for(int setIdx = 0; setIdx < minimalSolutionSets.size(); setIdx++) {
            Set<Integer> solutionSet = minimalSolutionSets.get(setIdx);
            List<Integer> nodeList = new ArrayList<>(solutionSet);

            System.out.println("    Processing solution hyperedge " + (setIdx + 1) +
                    " with " + nodeList.size() + " nodes: " + solutionSet);

            // 在解决方案集合内的每对节点之间添加边（形成完全子图）
            for(int i = 0; i < nodeList.size(); i++) {
                for(int j = i + 1; j < nodeList.size(); j++) {
                    int id1 = nodeList.get(i) + 1; // 转换为1基索引
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

        System.out.println("Solution-conflict graph written to: " + out);

        // 4. 输出图的详细信息
        outputGraphStatistics(facts, minimalSolutionSets, conflictEdges, solutionEdges);
    }

    // 输出图的统计信息
    static void outputGraphStatistics(List<Fact> facts,
                                      List<Set<Integer>> minimalSolutionSets,
                                      int conflictEdges, int solutionEdges) {
        System.out.println("\n=== Solution-Conflict Graph Statistics ===");
        System.out.println("Nodes (facts): " + facts.size());
        System.out.println("Conflict edges: " + conflictEdges);
        System.out.println("Solution edges: " + solutionEdges);
        System.out.println("Total edges: " + (conflictEdges + solutionEdges));
        System.out.println("Solution hyperedges: " + minimalSolutionSets.size());

        System.out.println("\nDetailed solution hyperedges:");
        for(int i = 0; i < minimalSolutionSets.size(); i++) {
            Set<Integer> solutionSet = minimalSolutionSets.get(i);
            System.out.println("  Hyperedge " + (i+1) + ": " + solutionSet +
                    " (size=" + solutionSet.size() + ", edges=" +
                    (solutionSet.size() * (solutionSet.size()-1) / 2) + ")");

            // 显示涉及的事实内容
            System.out.println("    Facts involved:");
            for(Integer idx : solutionSet) {
                if(idx < facts.size()) {
                    Fact fact = facts.get(idx);
                    System.out.println("      [" + idx + "] " + fact);
                }
            }
        }
        System.out.println("==========================================\n");
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

            // 找到最小解决方案集合（根据论文定义）
            List<Set<Integer>> minimalSolutionSets = findMinimalSolutionSets(
                    facts, Path.of(QUERY_DIR, baseName + ".q"), headers);

            if(minimalSolutionSets.isEmpty()) {
                System.out.println("WARNING: No minimal solution sets found for query!");
                System.out.println("This might indicate:");
                System.out.println("  1. The query file doesn't exist or is empty");
                System.out.println("  2. The query syntax is not recognized");
                System.out.println("  3. No facts in the database satisfy the query");

                // 创建空的解冲突图（只有冲突边）
                minimalSolutionSets = new ArrayList<>();
            }

            System.out.println("\nFinal minimal solution sets: " + minimalSolutionSets.size());

            // 生成解冲突图
            Path graphPath = Path.of(OUT_DIR, baseName + "_solution_conflict_graph.gr");
            writeSolutionConflictGraph(facts, fds, dcs, minimalSolutionSets, graphPath);

            // 计算树宽
            try {
                Path treeDecompositionPath = Path.of(OUT_DIR, baseName + "_result.td");
                ExactTW.main(new String[]{graphPath.toString(), treeDecompositionPath.toString(), "-acsd"});
                int treewidth = readTw(treeDecompositionPath);
                writeTw(treewidth, Path.of(OUT_DIR, baseName + "_treewidth.txt"));
                System.out.println("Computed treewidth: " + treewidth);
            } catch(Exception e) {
                System.err.println("Error computing treewidth: " + e.getMessage());
            }

            System.out.println("Completed processing: " + baseName);
        }
        System.out.println("\nAll files processed successfully!");
    }
}

