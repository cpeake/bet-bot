__version__ = 0.00

# NOTE: this setting scales up the size of betting across all strategies (USE WITH CAUTION!!!)
stake_multiplier = 1.0

# The minimum stake a bet can be placed at on Betfair
minimum_stake = 2.0

# Increment ladder for scaling up bet stakes
stake_ladder = [1.0, 1.0, 2.0, 4.0, 8.0, 16.0]
# stake_ladder = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]  # Safer strategy testing!

# Increment ladder for scaling up bet weights
weight_ladder = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
# weight_ladder = [1.0, 2.0, 3.0, 4.0, 5.0]  # Safer strategy testing!
