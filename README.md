# Features
- Solution-Conflict Graph  
- Tree Decomposition  
- Treewidth Report  
---
## Directory Layout
Directory Layout:
project‑root/
├── csv/       #  input tables  (name.csv)
├── fd/        # functional dependencies (name.fd)
├── dc/        # denial constraints     (name.dc)
├── q/     #  selection predicates   (name.q)
└── out/       #   generated graphs & td files
---
## Input File Formats
### `*.csv`
Ordinary comma-separated table.  
First row = header.  
Each subsequent row is treated as one Fact.
---
### `*.fd`
One functional dependency per line:  
A,B -> C
Left‑hand side attributes separated by commas, arrow ->, then a single right‑hand side attribute.
---
### `*.dc`
Denial constraints written in conjunctive form, e.g. 
¬( t1.A = t2.A && t1.B != t2.B )
Supported operators: =  !=  <  <=  >  >= && ||.
---
### `*.q`
monotonic Boolean query q(BCQ,BCQ!=,BUCQ!=),e.g.
R(x1, "Charlie", "CS101", x2, x3, x4) && R(y1, "Bob", "CS101", y2, y3, y4) && (x2 != y2)
Supported operators:&&(∧), ||(∨), =, != .
---
## Output Overview
result/
├── xxx_result.td
├──xxx_treewidth.txt
└── xxx_solution_conflict_graph.gr
---