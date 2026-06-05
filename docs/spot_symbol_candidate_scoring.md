# Spot Symbol Candidate Scoring

Symbol scores are decision aids for manual review. They never enable a symbol automatically.

The score combines TakerTaker profitability, MakerTaker expected value, opportunity frequency, duration, executable depth, fee confidence, book health, inventory efficiency, rebalance cost, and residual risk.

A high raw spread alone is insufficient. The scorer penalizes fallback fees, stale books, shallow depth, low sample count, high rebalance cost, and weak MakerTaker fill confidence.
