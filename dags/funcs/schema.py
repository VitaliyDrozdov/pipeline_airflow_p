#      ------> task_1a --> tast_1b ---               ------> task_4a --> tast_4b ---
#         /                                \            /                                \
# Start ---------> task_2a --> tast_2b --------> Step ---------> task_5a --> tast_5b --------> End
#         \                                                                                   /
#          ------> task_3a --> tast_3b ----------------------------------------> task_3c ----


#          -----> analyze_age ----------> create_age_ranges     ------> expenses_by_age_range   ----------------------------->
#         /                                             \     /                                                                    \
# Start ---> analyze_income --> analyze_occupation ---> Step ---------> credit_rating_by_age_range --> occupation_by_age_range ------> End
#         \                                                                                                                         /
#           ------> analyze_investment ---------> analyze_months-------------------------> investment_by_month -------------------->
