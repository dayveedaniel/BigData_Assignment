import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

path = os.getcwd()

FILES_COUNT = 2

def main():
    means_list = []
    std_list = []
    
    # Process each CSV file
    for i in range(FILES_COUNT):
        file = os.path.join(path, "output", f"q{i+1}.csv")
        df = pd.read_csv(file)
        means = df.mean()
        stds = df.std()
        print(f"Statistics for {file}:")
        for col in df.columns:
            print(f"  {col}: mean = {means[col]:.3f}, std = {stds[col]:.3f}")
        print()
        means_list.append(means)
        std_list.append(stds)
    
    means_df = pd.DataFrame(means_list, index=[f"Task {i+1}" for i in range(FILES_COUNT)])
    
    databases = means_df.columns.tolist()  
    queries = means_df.index.tolist()       
    n_queries = len(queries)
    n_databases = len(databases)
    
    bar_width = 0.2
    x = np.arange(n_queries)
    
    colors = ['skyblue', 'salmon', 'lightgreen'] 
    
    fig, ax = plt.subplots(figsize=(8, 6))
    
    for i, db in enumerate(databases):
        positions = x + i * bar_width - bar_width * (n_databases - 1) / 2
        bars = ax.bar(positions, means_df[db], width=bar_width, color=colors[i], label=db)
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:.4f}',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), 
                        textcoords="offset points",
                        ha='center', va='bottom')
    
    # Set x-axis labels and title
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.set_ylabel("Average Time")
    ax.set_title("Average Database Query Times")
    # Add legend with color mapping for databases
    ax.legend(title="Database")
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
        main()
