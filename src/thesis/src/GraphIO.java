package thesis.src;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

public class GraphIO {

    /** 读取 .gr （p tw n m，随后每行是一条边/超边，1 基 -> 0 基） */
    public static class GrData {
        public int nDeclared = -1;
        public int maxSeen = -1;
        public final List<BitSet> edges = new ArrayList<>();
    }

    public static GrData readGr(Path path) throws IOException {
        GrData d = new GrData();
        try (BufferedReader br = Files.newBufferedReader(path)) {
            String line; boolean afterP=false;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("c") || line.startsWith("#")) continue;
                if (line.startsWith("p ")) {
                    String[] t = line.split("\\s+");
                    if (t.length >= 4) d.nDeclared = Integer.parseInt(t[2]); // p tw n m
                    afterP = true; continue;
                }
                if (!afterP) continue;
                String[] toks = line.split("\\s+");
                BitSet e = new BitSet();
                for (String tok : toks) {
                    int v = Integer.parseInt(tok);
                    d.maxSeen = Math.max(d.maxSeen, v);
                    e.set(v - 1);
                }
                d.edges.add(e);
            }
        }
        return d;
    }

    /** 合成 Hypergraph（Ec=conflicts.gr, Es=solutions.gr） */
    public static Dynmaic_Programming_Based_for_CQA.Hypergraph loadHypergraph(Path conflictsGr, Path solutionsGr) throws IOException {
        GrData C = readGr(conflictsGr);
        GrData S = readGr(solutionsGr);
        int n = Math.max(Math.max(C.nDeclared, S.nDeclared), Math.max(C.maxSeen, S.maxSeen));
        if (n < 0) throw new IllegalArgumentException("gr 文件缺少 'p tw n m' 行");
        return new Dynmaic_Programming_Based_for_CQA.Hypergraph(n, C.edges, S.edges);
    }

    /** 读取 .td（s td ... / b <id> <verts...> / 袋间边）-> TreeDecomposition */
    public static Dynmaic_Programming_Based_for_CQA.TreeDecomposition loadTreeDecomposition(Path tdPath) throws IOException {
        Map<Integer, List<Integer>> bagVerts1Based = new HashMap<>();
        Map<Integer, List<Integer>> adj = new HashMap<>();
        int rootId = -1;

        try (BufferedReader br = Files.newBufferedReader(tdPath)) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("c")) continue;
                if (line.startsWith("s ")) continue;
                if (line.startsWith("b ")) {
                    String[] t = line.split("\\s+");
                    int id = Integer.parseInt(t[1]);
                    List<Integer> verts = new ArrayList<>();
                    for (int i = 2; i < t.length; i++) verts.add(Integer.parseInt(t[i]));
                    bagVerts1Based.put(id, verts);
                    adj.putIfAbsent(id, new ArrayList<>());
                    if (rootId == -1 || verts.size() > bagVerts1Based.get(rootId).size()) rootId = id;
                    continue;
                }
                String[] t = line.split("\\s+");
                if (t.length == 2 && Character.isDigit(t[0].charAt(0))) {
                    int u = Integer.parseInt(t[0]), v = Integer.parseInt(t[1]);
                    adj.putIfAbsent(u, new ArrayList<>()); adj.putIfAbsent(v, new ArrayList<>());
                    adj.get(u).add(v); adj.get(v).add(u);
                }
            }
        }
        if (bagVerts1Based.isEmpty()) throw new IllegalArgumentException("TD 文件里没有任何袋（b ...）");
        if (rootId == -1) rootId = bagVerts1Based.keySet().iterator().next();

        Map<Integer, Dynmaic_Programming_Based_for_CQA.TDNode> bags = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> e : bagVerts1Based.entrySet()) {
            int id = e.getKey();
            int[] elems0 = e.getValue().stream().distinct().sorted().mapToInt(v -> v - 1).toArray();
            bags.put(id, new Dynmaic_Programming_Based_for_CQA.TDNode(id, elems0));
        }

        Set<Integer> seen = new HashSet<>();
        ArrayDeque<Integer> dq = new ArrayDeque<>();
        dq.add(rootId); seen.add(rootId);
        while (!dq.isEmpty()) {
            int u = dq.poll();
            for (int v : adj.getOrDefault(u, List.of())) {
                if (seen.add(v)) {
                    bags.get(u).addChild(bags.get(v));
                    dq.add(v);
                }
            }
        }
        return new Dynmaic_Programming_Based_for_CQA.TreeDecomposition(bags.get(rootId));
    }

    /** 可选：校验 Ec∪Es 的每条（超）边是否被某个袋覆盖 */
    public static void assertEdgeCoverage(Dynmaic_Programming_Based_for_CQA.Hypergraph H,
                                          Dynmaic_Programming_Based_for_CQA.TreeDecomposition TD) {
        List<Dynmaic_Programming_Based_for_CQA.TDNode> bags = new ArrayList<>(TD.nodes.values());
        List<BitSet> all = new ArrayList<>();
        all.addAll(H.conflictEdges); all.addAll(H.solutionEdges);
        for (int i = 0; i < all.size(); i++) {
            BitSet e = all.get(i);
            boolean covered = false;
            for (var b : bags) {
                BitSet bagSet = new BitSet();
                for (int v : b.bagElems) bagSet.set(v);
                BitSet t = (BitSet) e.clone(); t.andNot(bagSet);
                if (t.isEmpty()) { covered = true; break; }
            }
            if (!covered) throw new IllegalStateException("Edge #" + i + " 不在任何袋内（TD 不是联合图或团化不完整）");
        }
    }
}
