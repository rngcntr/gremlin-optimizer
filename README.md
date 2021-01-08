# STRIPE
This repository contains the implementation of the publication "STRIPE: A Graph-Aware Optimizer for Gremlin Queries".
All contents are currently 'proof of concept' and not declared ready for productive use.

### What is this?
The goal of this project is to allow graph-aware optimization of Gremlin queries.
To do so, input queries are parsed into an abstract pattern-matching structure, on which much more powerful optimizations can be performed, compared to plain gremlin.
Afterwards, the optimized pattern is serialized back into a Gremlin representation for execution.

### How to use it?
This project was designed to be integrated by graph database vendors into their usual query optimization procecdure.
To do it's work, the optimizer requires the graph database to provide a predefined set of statistical information.

Usage examples are provided in a separate repository: https://github.com/rngcntr/stripe-evaluation
