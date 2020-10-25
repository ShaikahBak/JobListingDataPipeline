# importing pandas as pd
import pandas as pd

# Read and store content
# of an excel file
read_file = pd.read_excel("data/data_tools.xlsx")

# Write the dataframe object
# into csv file
read_file.to_csv("data/data_tools.csv",
                 index=None,
                 header=True)

# # read csv file and convert
# # into a dataframe object
# df = pd.DataFrame(pd.read_csv("Test.csv"))
#
# # show the dataframe
# df