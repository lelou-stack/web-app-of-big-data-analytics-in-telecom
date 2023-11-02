import pandas as pd
import matplotlib.pyplot as plt

# Read the CSV file
df = pd.read_csv('./remaining_calls/remainingcalls.csv/part-00000-045dc79a-f69b-4ffb-86d3-903186a2a6f7-c000.csv')

# Use the row index as customer ID (starting from 1)
customer_ids = range(1, len(df) + 1)

# Extract the 'Remaining_Calls' column
remaining_calls = df['Remaining_Calls']

# Create a scatter plot
plt.scatter(customer_ids, remaining_calls, marker='o', s=20)

# Set axis labels and title
plt.xlabel('Customer ID')
plt.ylabel('Remaining Calls')
plt.title('Remaining Calls for Each Customer')

# Display the plot
plt.show()
