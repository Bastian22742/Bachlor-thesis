package thesis.src;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import io.github.twalgor.main.ExactTW;
/* * I dont know why that, if i dont use Src folder as a package, My Intelijj Idea can not identify my thesis-folder as a projcet*/
public class Main {


    private static final String CSV_DIR   = "./csv_inputs";
    private static final String FD_DIR    = "./fd";
    private static final String DC_DIR    = "./dc";
    private static final String QUERY_DIR = "./query";
    private static final String OUT_DIR   = "./result";

    /* * defined Fact class which is the data read from database*/
    static class Fact {
        final Map<String,String> map = new HashMap<>();
        Fact(String[] header, String[] values){
            for(int i=0;i<header.length;i++)
                map.put(header[i].trim(),
                        i < values.length ? values[i].trim() : null);//Use trim here for avoiding empty content from a Cell ,for exapmle “ 30",and also handling values less than header (put null in cell)
        }
        String get(String k){ return map.get(k); }
    }
    /* Denial Constraint construct from dc Folie*/
    static class DCAtom{
        String lAttr, op, rAttr, constVal;
        boolean isConst, sameTuple;            // sameTuple=true → t1.A operation t1.B
        // t1.A operation t2.B
        DCAtom(String l,String o,String r){
            lAttr=l; op=o; rAttr=r;
            isConst=false; sameTuple=false;
        }

        DCAtom(String l,String o,String rhs, boolean isConst, boolean same){
            lAttr=l; op=o; this.isConst=isConst; this.sameTuple=same;
            if(isConst) constVal=rhs; else rAttr=rhs;
        }
    }

    /* read fact from CSV*/
    static List<Fact> readFacts(Path csv) throws IOException{
        List<Fact> list=new ArrayList<>();
        try(BufferedReader br=Files.newBufferedReader(csv)){
            String header=br.readLine();
            if(header==null) return list;//if header is 0,that means thie CSV is empty ,we dont need to do otherthings
            String[] h = header.split(",");// get the header
            String line;
            while((line=br.readLine())!=null)
                list.add(new Fact(h, line.split(",")));//Create each Cell a Fact
        }
        return list;
    }

    /* read FD*/
    static List<Map.Entry<List<String>,String>> readFD(Path fd) throws IOException{
        List<Map.Entry<List<String>,String>> list=new ArrayList<>();
        if(!Files.exists(fd)) return list;
        try(BufferedReader br=Files.newBufferedReader(fd)){
            String l;
            while((l=br.readLine())!=null){
                l=l.trim();//if line is empty,then jump
                if(l.isEmpty()||l.startsWith("#")) continue;
                String[] parts=l.split("->");
                if(parts.length!=2) continue;
                list.add(new AbstractMap.SimpleEntry<>( //Construct the FD rules,the form of fd
                        Arrays.asList(parts[0].trim().split(",")),
                        parts[1].trim()));
            }
        }
        return list;
    }

    /* check funktion,check whether the data violate the fd*/
    static boolean violatesFD(List<String> lhs,String rhs,Fact f1,Fact f2){
        for(String a: lhs){
            String v1=f1.get(a), v2=f2.get(a);
            if(v1 == null || !v1.equals(v2)) return false;
        }//check if the left attribute are same?
        String r1=f1.get(rhs), r2=f2.get(rhs);//chek if the right attribute are different?
        return r1!=null && r2!=null && !r1.equals(r2);
    }

    /* read dc from Class DCatom */
    static List<List<DCAtom>> readDC(Path dc) throws IOException{
        List<List<DCAtom>> dcs=new ArrayList<>();
        if(!Files.exists(dc)) return dcs;
        Pattern pat = Pattern.compile(
                "t1\\.([A-Za-z0-9_ ()%-]+)([!=><]{1,2})(?:t2\\.([A-Za-z0-9_ ()%-]+)|t1\\.([A-Za-z0-9_ ()%-]+)|([0-9.]+)|\"([^\"]+)\")"
        );

        try(BufferedReader br=Files.newBufferedReader(dc)){
            String l;
            while((l=br.readLine())!=null){
                l=l.replaceAll("\\s+","");           // delete the empty variante
                if(l.isEmpty()||l.startsWith("#")) continue;
                if(l.startsWith("¬(")) l=l.substring(2,l.length()-1); // delete ¬(...)
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
    /* Its just Number compare function,some number in CSV are typ: String,we need to compare them, So we construct one */
    static int cmpNum(String a,String b){
        try{ return Double.compare(Double.parseDouble(a),Double.parseDouble(b)); }
        catch(Exception e){ return a.compareTo(b); }
    }
    /* Its just Compare function for DC compare */
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
    /* Its a function to check whether the data violate dc  */
    static boolean violatesDC(List<DCAtom> clause,Fact f1,Fact f2){
        for(DCAtom a: clause) if(!sat(a,f1,f2)) return false;
        return true;
    }
    /* get the query from query folie,and find the data which is pass to the query */
    static Set<Integer> queryIdx(List<Fact> facts,Path q) throws IOException{
        Set<Integer> idx=new HashSet<>();
        if(!Files.exists(q)) return idx;// read all constraint from query foile
        Map<String,Set<String>> cond=new HashMap<>();
        try(BufferedReader br=Files.newBufferedReader(q)){
            String l;
            while((l=br.readLine())!=null){
                if(!l.contains("=")) continue;
                String[] p=l.split("=");
                cond.put(p[0].trim(),
                        new HashSet<>(Arrays.asList(p[1].trim().split(","))));
            }
        }
        for(int i=0;i<facts.size();i++){// go through all fact here
            Fact f=facts.get(i); boolean ok=true;
            for(String k:cond.keySet()){
                String v=f.get(k);
                if(v==null||!cond.get(k).contains(v)){ ok=false; break; }
            }
            if(ok) idx.add(i);
        }
        return idx;
    }
    /* construct the conflict graph */
    static void writeGr(List<Fact> facts,
                        List<Map.Entry<List<String>,String>> fds,
                        List<List<DCAtom>> dcs,
                        Path out, Set<Integer> filter) throws IOException{

        List<Integer> map = (filter==null)? null : new ArrayList<>(filter);
        List<Fact> tg     = (filter==null)? facts: new ArrayList<>();
        if(filter!=null) for(int id:filter) tg.add(facts.get(id));

        List<int[]> edges=new ArrayList<>();

        for(int i=0;i<tg.size();i++){ //construct the conflict edges
            for(int j=i+1;j<tg.size();j++){
                Fact f1=tg.get(i), f2=tg.get(j);
                boolean conf=false;

                for(var fd:fds){
                    if(violatesFD(fd.getKey(),fd.getValue(),f1,f2)){ conf=true; break; }
                }
                if(!conf){
                    for(var dc:dcs){
                        if(violatesDC(dc,f1,f2)){ conf=true; break; }
                    }
                }
                if(conf){
                    int id1 = (filter==null)? i+1 : map.get(i)+1;
                    int id2 = (filter==null)? j+1 : map.get(j)+1;
                    edges.add(new int[]{id1,id2});
                }
            }
        }

        Files.createDirectories(out.getParent());
        try(BufferedWriter bw=Files.newBufferedWriter(out)){
            int n = (filter==null)? facts.size() : filter.size();
            bw.write("p tw " + n + " " + edges.size()); bw.newLine();
            for(int[] e: edges) bw.write(e[0] + " " + e[1] + "\n");
        }
    }

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
            var fds = readFD(Path.of(FD_DIR, base + ".fd"));
            var dcs = readDC(Path.of(DC_DIR, base + ".dc"));
            var q   = queryIdx(facts, Path.of(QUERY_DIR, base + ".query"));//all facts which is passed to query condtion


            Path g = Path.of(OUT_DIR, base + "_conflict_graph.gr");
            writeGr(facts, fds, dcs, g, null);


            Path td = Path.of(OUT_DIR, base + "_result.td");
            ExactTW.main(new String[]{g.toString(), td.toString(), "-acsd"});
            writeTw(readTw(td),
                    Path.of(OUT_DIR, base + "_treewidth.txt"));

            if(!q.isEmpty()){
                Path sg = Path.of(OUT_DIR,//if find at least 1 result rely on query condition
                        base + "_solution_conflict_graph.gr");
                writeGr(facts, fds, dcs, sg, q);
            }
        }
    }
}






