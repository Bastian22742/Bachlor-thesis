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
        Map<String, String> constantConditions; // 具体的属性值条件
        Map<String, String> variableBindings;   // 属性 -> 变量名
        String atomName;  // 用于调试

        QueryAtomGGM(int atomIndex, String atomName) {
            this.atomIndex = atomIndex;
            this.atomName = atomName;
            this.constantConditions = new HashMap<>();
            this.variableBindings = new HashMap<>();
        }

        void addConstantCondition(String attribute, String value) {
            constantConditions.put(attribute, value);
        }

        void addVariableBinding(String attribute, String variable) {
            variableBindings.put(attribute, variable);
        }

        // 获取所有变量
        Set<String> getVariables() {
            Set<String> vars = new HashSet<>(variableBindings.values());
            return vars;
        }

        boolean matchesFact(Fact fact, String[] headers) {
            // 检查常量条件
            for(String attr : constantConditions.keySet()) {
                String requiredValue = constantConditions.get(attr);
                String actualValue = fact.get(attr);
                if(actualValue == null || !actualValue.equals(requiredValue)) {
                    return false;
                }
            }
            return true;
        }

        // 从事实中提取变量绑定
        Map<String, String> extractVariableBindings(Fact fact) {
            Map<String, String> bindings = new HashMap<>();
            for(String attr : variableBindings.keySet()) {
                String variable = variableBindings.get(attr);
                String value = fact.get(attr);
                if(value != null) {
                    bindings.put(variable, value);
                }
            }
            return bindings;
        }

        @Override
        public String toString() {
            return atomName + ": constants=" + constantConditions + ", variables=" + variableBindings;
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

    /* ---------- Conjunctive Query (复用SCG的逻辑) ---------- */
    static class ConjunctiveQuery {
        List<QueryAtomGGM> atoms;
        List<VariableConstraint> variableConstraints;

        ConjunctiveQuery(List<QueryAtomGGM> atoms, List<VariableConstraint> variableConstraints) {
            this.atoms = atoms;
            this.variableConstraints = variableConstraints;
        }

        // 检查给定的事实集合是否满足这个合取查询
        boolean isSatisfiedBy(List<Fact> facts, Set<Integer> indices, String[] headers) {
            // 为每个原子找到匹配的事实
            List<List<Integer>> atomMatches = new ArrayList<>();

            for(QueryAtomGGM atom : atoms) {
                List<Integer> matches = new ArrayList<>();
                for(Integer idx : indices) {
                    Fact fact = facts.get(idx);
                    if(atom.matchesFact(fact, headers)) {
                        matches.add(idx);
                    }
                }
                atomMatches.add(matches);

                // 如果某个原子没有匹配的事实，则整个查询失败
                if(matches.isEmpty()) {
                    return false;
                }
            }

            // 尝试所有可能的变量绑定组合
            return tryAllCombinations(atomMatches, 0, new ArrayList<>(), facts);
        }

        private boolean tryAllCombinations(List<List<Integer>> atomMatches, int atomIndex,
                                           List<Integer> currentAssignment, List<Fact> facts) {
            if(atomIndex == atomMatches.size()) {
                // 检查当前分配是否满足所有变量约束
                return checkVariableConstraints(currentAssignment, facts);
            }

            // 尝试当前原子的所有可能匹配
            for(Integer factIndex : atomMatches.get(atomIndex)) {
                currentAssignment.add(factIndex);
                if(tryAllCombinations(atomMatches, atomIndex + 1, currentAssignment, facts)) {
                    return true;
                }
                currentAssignment.remove(currentAssignment.size() - 1);
            }

            return false;
        }

        private boolean checkVariableConstraints(List<Integer> factIndices, List<Fact> facts) {
            // 收集所有变量绑定
            Map<String, String> allBindings = new HashMap<>();

            for(int i = 0; i < factIndices.size(); i++) {
                Integer factIndex = factIndices.get(i);
                Fact fact = facts.get(factIndex);
                QueryAtomGGM atom = atoms.get(i);

                Map<String, String> atomBindings = atom.extractVariableBindings(fact);

                // 检查变量绑定的一致性
                for(String variable : atomBindings.keySet()) {
                    String value = atomBindings.get(variable);
                    if(allBindings.containsKey(variable)) {
                        // 如果变量已经有绑定，检查是否一致
                        if(!allBindings.get(variable).equals(value)) {
                            return false;  // 变量绑定不一致
                        }
                    } else {
                        allBindings.put(variable, value);
                    }
                }
            }

            // 检查所有变量约束
            for(VariableConstraint constraint : variableConstraints) {
                if(!constraint.isSatisfied(allBindings)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public String toString() {
            return atoms.toString() + " with constraints: " + variableConstraints.toString();
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

    /* ---------- 改进的查询解析 (兼容SCG格式) ---------- */
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

            if(query.contains("R(") || query.contains("T(")) {
                return parseRelationalQueryForGGM(query, headers);
            } else {
                return parseAttributeValueQueryForGGM(query);
            }
        }
    }

    static ParsedQuery parseRelationalQueryForGGM(String query, String[] headers) {
        List<QueryAtomGGM> queryAtoms = new ArrayList<>();
        List<VariableConstraint> variableConstraints = new ArrayList<>();

        // 解析关系原子 R(...) 或 T(...)
        Pattern relationPattern = Pattern.compile("([RT])\\(([^)]+)\\)");
        Matcher matcher = relationPattern.matcher(query);

        int atomIndex = 0;
        while (matcher.find()) {
            String relationName = matcher.group(1);
            String params = matcher.group(2).trim();
            QueryAtomGGM atom = new QueryAtomGGM(atomIndex, "Atom" + atomIndex);

            // 判断是否为命名参数模式
            if (params.contains("=")) {
                // 命名参数模式: Attr = value
                String[] parts = params.split(",");
                for (String part : parts) {
                    part = part.trim();
                    if (part.contains("=")) {
                        String[] kv = part.split("=", 2);
                        String attr = kv[0].trim();
                        String value = cleanValue(kv[1].trim());

                        if (isVariable(value)) {
                            atom.addVariableBinding(attr, value);
                        } else if (!value.isEmpty()) {
                            atom.addConstantCondition(attr, value);
                        }
                    }
                }
            } else {
                // 位置参数模式：按位置对应 headers
                String[] values = params.split(",");
                for (int i = 0; i < values.length && i < headers.length; i++) {
                    String value = cleanValue(values[i].trim());
                    if (isVariable(value)) {
                        atom.addVariableBinding(headers[i], value);
                    } else if (!value.isEmpty()) {
                        atom.addConstantCondition(headers[i], value);
                    }
                }
            }

            queryAtoms.add(atom);
            System.out.println("  Parsed query atom " + atomIndex + ": " + atom);
            atomIndex++;
        }

        // 解析变量约束
        Pattern constraintPattern = Pattern.compile("\\(\\s*(\\w+)\\s*([!=<>]+)\\s*(\\w+)\\s*\\)");
        Matcher constraintMatcher = constraintPattern.matcher(query);
        while (constraintMatcher.find()) {
            String leftVar = constraintMatcher.group(1);
            String operator = constraintMatcher.group(2);
            String rightVar = constraintMatcher.group(3);
            variableConstraints.add(new VariableConstraint(leftVar, operator, rightVar));
            System.out.println("  Parsed variable constraint: " + leftVar + " " + operator + " " + rightVar);
        }

        return new ParsedQuery(queryAtoms, variableConstraints);
    }

    static ParsedQuery parseAttributeValueQueryForGGM(String query) {
        List<QueryAtomGGM> queryAtoms = new ArrayList<>();
        List<VariableConstraint> variableConstraints = new ArrayList<>();

        String[] orParts = query.split("\\|\\|");

        int atomIndex = 0;
        for(String orPart : orParts) {
            orPart = orPart.trim();

            List<Map<String, String>> alternatives = expandAlternatives(orPart);

            for(Map<String, String> conditions : alternatives) {
                if(!conditions.isEmpty()) {
                    QueryAtomGGM atom = new QueryAtomGGM(atomIndex, "SimpleAtom" + atomIndex);
                    for(String attr : conditions.keySet()) {
                        atom.addConstantCondition(attr, conditions.get(attr));
                    }
                    queryAtoms.add(atom);
                    System.out.println("  Parsed attribute-value query atom " + atomIndex + ": " + conditions);
                    atomIndex++;
                }
            }
        }

        return new ParsedQuery(queryAtoms, variableConstraints);
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
        if(value == null || value.isEmpty()) return false;
        if ((value.startsWith("\"") && value.endsWith("\"")) ||
                (value.startsWith("'") && value.endsWith("'"))) {
            return false;
        }
        return value.equals("_") ||
                value.equals("*") ||
                value.matches("^[a-z][0-9]*$");       // var, var1, etc.
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

    /* ---------- GGM Join边计算 (修正版本) ---------- */

    // 检查两个查询原子是否q-linked
    static boolean areQueryAtomsLinked(QueryAtomGGM atom1, QueryAtomGGM atom2,
                                       List<VariableConstraint> variableConstraints) {
        System.out.println("    Checking if q" + atom1.atomIndex + " and q" + atom2.atomIndex + " are q-linked:");

        // 条件1: 相同的原子索引
        if(atom1.atomIndex == atom2.atomIndex) {
            System.out.println("      Same atom index - q-linked");
            return true;
        }

        // 条件2: 变量有交集
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

    // 检查两个事实是否q-consistent
    static boolean areFactsQConsistent(Fact fact1, Fact fact2, QueryAtomGGM atom1, QueryAtomGGM atom2,
                                       List<VariableConstraint> variableConstraints, String[] headers) {

        // 首先检查基本匹配
        if(!atom1.matchesFact(fact1, headers)) {
            return false;
        }
        if(!atom2.matchesFact(fact2, headers)) {
            return false;
        }

        // 建立变量绑定
        Map<String, String> variableBindings = new HashMap<>();

        // 为atom1建立变量绑定
        Map<String, String> bindings1 = atom1.extractVariableBindings(fact1);
        variableBindings.putAll(bindings1);

        // 为atom2建立变量绑定，检查冲突
        Map<String, String> bindings2 = atom2.extractVariableBindings(fact2);
        for(String var : bindings2.keySet()) {
            String value = bindings2.get(var);
            String existingValue = variableBindings.get(var);
            if(existingValue != null && !existingValue.equals(value)) {
                return false; // 变量绑定冲突
            }
            variableBindings.put(var, value);
        }

        // 检查所有相关的变量约束
        for(VariableConstraint constraint : variableConstraints) {
            // 检查约束是否与这两个原子相关
            Set<String> vars1 = atom1.getVariables();
            Set<String> vars2 = atom2.getVariables();

            boolean constraintInvolvesAtom1 = vars1.contains(constraint.leftVar) || vars1.contains(constraint.rightVar);
            boolean constraintInvolvesAtom2 = vars2.contains(constraint.leftVar) || vars2.contains(constraint.rightVar);

            if(constraintInvolvesAtom1 && constraintInvolvesAtom2) {
                if(!constraint.isSatisfied(variableBindings)) {
                    return false;
                }
            }
        }
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

        // 2. 添加Join边
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
                    continue;
                }

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

            // 解析查询（现在兼容SCG格式）
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