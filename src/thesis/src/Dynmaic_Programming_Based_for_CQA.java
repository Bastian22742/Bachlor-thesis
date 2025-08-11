package thesis.src;

import java.math.BigInteger;
import java.util.*;

public class Dynmaic_Programming_Based_for_CQA {

    /** Hypergraph = facts + Ec + Es */
    public static final class Hypergraph {
        public final int n;
        public final List<BitSet> conflictEdges;  // Ec
        public final List<BitSet> solutionEdges;  // Es
        public Hypergraph(int n, List<BitSet> conflictEdges, List<BitSet> solutionEdges) {
            this.n = n; this.conflictEdges = conflictEdges; this.solutionEdges = solutionEdges;
        }
    }

    /** TD node (bag) */
    public static final class TDNode {
        public final int id;
        public final int[] bagElems;                 // 0-based fact ids
        public final List<TDNode> children = new ArrayList<>();
        List<BitSet> conflictEdgesInBag;
        List<BitSet> solutionEdgesInBag;
        public TDNode(int id, int[] bagElems){ this.id=id; this.bagElems=bagElems; }
        public void addChild(TDNode child){ children.add(child); }
    }

    /** TD wrapper with root */
    public static final class TreeDecomposition {
        public final TDNode root;
        public final Map<Integer, TDNode> nodes = new HashMap<>();
        public TreeDecomposition(TDNode root){ this.root=root; collect(root); }
        private void collect(TDNode u){ nodes.put(u.id,u); for(TDNode v:u.children) collect(v); }
    }

    /* ========================= Engine ========================= */

    public static final class Engine {
        private final Hypergraph H;
        private final TreeDecomposition T;

        private final Map<Integer, Map<Integer,Integer>> localIndex = new HashMap<>();
        private final Map<Long, BCMapping> bcMap = new HashMap<>();
        private final Map<FKey, BigInteger> fMemo = new HashMap<>();
        private final Map<GKey, BigInteger> gMemo = new HashMap<>();

        public Engine(Hypergraph H, TreeDecomposition T){ this.H=H; this.T=T; prepare(); }

        public BigInteger numberFalsify() {
            TDNode a = T.root;
            int bagSize = a.bagElems.length;
            int allChildrenMask = (1 << a.children.size()) - 1;
            BigInteger total = BigInteger.ZERO;
            for (int rMask = 0; rMask < (1 << bagSize); rMask++) {
                int sMask = (1 << bagSize) - 1; // a⁺（maxrep 会约束）
                total = total.add(f(a, allChildrenMask, rMask, sMask));
            }
            return total;
        }

        /* ---------- prepare ---------- */

        private void prepare(){
            for (TDNode b : T.nodes.values()) {
                Map<Integer,Integer> m = new HashMap<>();
                for (int i = 0; i < b.bagElems.length; i++) m.put(b.bagElems[i], i);
                localIndex.put(b.id, m);
            }
            for (TDNode b : T.nodes.values()) {
                b.conflictEdgesInBag = new ArrayList<>();
                b.solutionEdgesInBag = new ArrayList<>();
                BitSet bagSet = new BitSet(H.n);
                for (int g : b.bagElems) bagSet.set(g);
                for (BitSet e : H.conflictEdges) if (isSubset(e, bagSet)) b.conflictEdgesInBag.add((BitSet)e.clone());
                for (BitSet e : H.solutionEdges) if (isSubset(e, bagSet)) b.solutionEdgesInBag.add((BitSet)e.clone());
            }
            for (TDNode b : T.nodes.values()) {
                for (int idx = 0; idx < b.children.size(); idx++) {
                    TDNode c = b.children.get(idx);
                    BCMapping map = new BCMapping(b, c, localIndex.get(b.id), localIndex.get(c.id));
                    bcMap.put(pack(b.id, idx), map);
                }
            }
        }

        /* ---------- DP ---------- */

        private BigInteger f(TDNode b, int Cmask, int rMask, int sMask){
            FKey key = new FKey(b.id, Cmask, rMask, sMask);
            BigInteger cached = fMemo.get(key);
            if (cached != null) return cached;

            if (Cmask == 0) {
                BigInteger res = h(b, rMask, sMask) ? BigInteger.ONE : BigInteger.ZERO;
                fMemo.put(key, res); return res;
            }

            int childIdx = Integer.numberOfTrailingZeros(Cmask);
            int restC = Cmask & ~(1 << childIdx);
            TDNode c = b.children.get(childIdx);
            BCMapping map = bcMap.get(pack(b.id, childIdx));

            int sInterB = sMask & map.maskBInter;
            int rInterB = rMask & map.maskBInter;

            int sInter = map.interMaskBToInterMask(sInterB);
            int rInter = map.interMaskBToInterMask(rInterB);
            int free = sInter & ~rInter;

            BigInteger sum = BigInteger.ZERO;
            for (int t = free; ; t = (t - 1) & free) {
                int s1  = rInter | t;
                int s2  = rInter | (free ^ t);

                int sMinusC_B = sMask & ~map.maskBInter;
                int s1_B = map.interMaskToBMask(s1);
                int sNew_B = sMinusC_B | s1_B;

                BigInteger left  = f(b, restC, rMask, sNew_B);
                BigInteger right = g(b, childIdx, rInter, s2);
                sum = sum.add(left.multiply(right));
                if (t == 0) break;
            }
            fMemo.put(key, sum); return sum;
        }

        private BigInteger g(TDNode b, int childIdx, int rInter, int sInter){
            GKey key = new GKey(b.id, childIdx, rInter, sInter);
            BigInteger cached = gMemo.get(key);
            if (cached != null) return cached;

            TDNode c = b.children.get(childIdx);
            int CmaskChild = (1 << c.children.size()) - 1;
            BCMapping map = bcMap.get(pack(b.id, childIdx));

            int rC = map.interMaskToCMask(rInter);
            int sC = map.interMaskToCMask(sInter);
            int sUnion = sC | map.maskCminusB;

            BigInteger sum = BigInteger.ZERO;
            int free = map.maskCminusB;
            int freeCount = Integer.bitCount(free);
            for (int idx = 0; idx < (1 << freeCount); idx++) {
                int rPrime = liftSubmask(idx, free);
                sum = sum.add(f(c, CmaskChild, rC | rPrime, sUnion));
            }
            gMemo.put(key, sum); return sum;
        }

        /* ---------- h & maxrep ---------- */

        private boolean h(TDNode b, int rMask, int sMask){
            if (containsEdge(b.conflictEdgesInBag, b, rMask)) return false;
            if (containsEdge(b.solutionEdgesInBag, b, rMask)) return false;
            Integer sComputed = maxrep(b, rMask);
            return sComputed != null && sComputed == sMask;
        }

        private Integer maxrep(TDNode b, int rMask){
            if (containsEdge(b.conflictEdgesInBag, b, rMask)) return null;
            int sMask = rMask;
            int all = (1 << b.bagElems.length) - 1;
            int rest = all & ~rMask;
            for (int bit = 0; bit < b.bagElems.length; bit++) {
                if (((rest >> bit) & 1) == 0) continue;
                int rPlus = rMask | (1 << bit);
                if (containsEdge(b.conflictEdgesInBag, b, rPlus)) sMask |= (1 << bit);
            }
            return sMask;
        }

        /* ---------- helpers ---------- */

        private static boolean containsEdge(List<BitSet> edgesInBag, TDNode b, int rMask){
            BitSet rSet = new BitSet();
            for (int i = 0; i < b.bagElems.length; i++) if (((rMask >> i) & 1) != 0) rSet.set(b.bagElems[i]);
            for (BitSet e : edgesInBag) if (isSubset(e, rSet)) return true;
            return false;
        }
        private static boolean isSubset(BitSet a, BitSet b){
            BitSet t = (BitSet)a.clone(); t.andNot(b); return t.isEmpty();
        }
        private static long pack(int a, int b){ return (((long)a) << 32) ^ (b & 0xffffffffL); }
        private static int liftSubmask(int idx, int freeMask){
            int res = 0, cnt = 0;
            for (int bit = 0; bit < 31; bit++) {
                if (((freeMask >> bit) & 1) == 0) continue;
                if (((idx >> cnt) & 1) != 0) res |= (1 << bit);
                cnt++;
            }
            return res;
        }

        /* ---------- map & keys ---------- */

        private static final class BCMapping {
            final int[] interToBBit, interToCBit;
            final int maskBInter, maskCminusB;
            BCMapping(TDNode b, TDNode c, Map<Integer,Integer> bLocal, Map<Integer,Integer> cLocal){
                List<Integer> inter = new ArrayList<>();
                Set<Integer> setB = new HashSet<>();
                for (int x : b.bagElems) setB.add(x);
                for (int y : c.bagElems) if (setB.contains(y)) inter.add(y);

                interToBBit = new int[inter.size()];
                interToCBit = new int[inter.size()];
                int mb=0, mc=0;
                for (int i = 0; i < inter.size(); i++) {
                    int g = inter.get(i);
                    int bb = bLocal.get(g), cb = cLocal.get(g);
                    interToBBit[i] = bb; interToCBit[i] = cb;
                    mb |= (1 << bb); mc |= (1 << cb);
                }
                maskBInter = mb;
                // c \ b
                int mCnotB = 0;
                for (int i = 0; i < c.bagElems.length; i++) if (!setB.contains(c.bagElems[i])) mCnotB |= (1 << i);
                maskCminusB = mCnotB;
            }
            int interMaskBToInterMask(int maskB){
                int m=0; for(int i=0;i<interToBBit.length;i++) if(((maskB>>interToBBit[i])&1)!=0) m|=(1<<i); return m;
            }
            int interMaskToBMask(int interMask){
                int m=0; for(int i=0;i<interToBBit.length;i++) if(((interMask>>i)&1)!=0) m|=(1<<interToBBit[i]); return m;
            }
            int interMaskToCMask(int interMask){
                int m=0; for(int i=0;i<interToCBit.length;i++) if(((interMask>>i)&1)!=0) m|=(1<<interToCBit[i]); return m;
            }
        }
        private static final class FKey{
            final int bagId,Cmask,rMask,sMask;
            FKey(int a,int b,int c,int d){bagId=a;Cmask=b;rMask=c;sMask=d;}
            public boolean equals(Object o){ if(!(o instanceof FKey))return false; FKey k=(FKey)o; return bagId==k.bagId&&Cmask==k.Cmask&&rMask==k.rMask&&sMask==k.sMask;}
            public int hashCode(){ return Objects.hash(bagId,Cmask,rMask,sMask); }
        }
        private static final class GKey{
            final int bagId,childIdx,rInter,sInter;
            GKey(int a,int b,int c,int d){bagId=a;childIdx=b;rInter=c;sInter=d;}
            public boolean equals(Object o){ if(!(o instanceof GKey))return false; GKey k=(GKey)o; return bagId==k.bagId&&childIdx==k.childIdx&&rInter==k.rInter&&sInter==k.sInter;}
            public int hashCode(){ return Objects.hash(bagId,childIdx,rInter,sInter); }
        }
    }

    /** helper to build BitSet edge from ints (0-based) */
    public static BitSet edge(int... nodes){
        BitSet bs = new BitSet(); for(int v: nodes) bs.set(v); return bs;
    }
}
