Features:
Conflict Graph
Solution‑Conflict Graph
Tree Decomposition
Treewidth Report

Directory Layout:
project‑root/
├── csv/       #  input tables  (name.csv)
├── fd/        # functional dependencies (name.fd)
├── dc/        # denial constraints     (name.dc)
├── query/     #  selection predicates   (name.query)
└── out/       #   generated graphs & td files

Input File Formats:
*.csv
Ordinary comma‑separated table. First row = header. Each subsequent row is treated as one Fact.
*.fd
One functional dependency per line:
A,B -> C
Left‑hand side attributes separated by commas, arrow ->, then a single right‑hand side attribute.
*.dc
Denial constraints written in conjunctive form, eg.
¬( t1.A = t2.A && t1.B != t2.B )
Supported operators: =  !=  <  <=  >  >=.
*.query
Selection predicates, one per line; each line
attribute = value1,value2,…
Values are OR‑combined within a line; different lines are AND‑combined.

Output Overview:
out/
├── xxx_conflict_graph.gr
├── xxx_result.td
├──xxx_treewidth.txt
└── xxx_solution_conflict_graph.gr