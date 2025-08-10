# Features
- Conflict Graph  
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
Supported operators: =  !=  <  <=  >  >=.
---
### `*.q`
In form BCQ!=
---
## Output Overview
result/
├── xxx_ggm_graph.gr
├── xxx_ggm_result.td
└── xxx__ggm_treewidth.txt
---