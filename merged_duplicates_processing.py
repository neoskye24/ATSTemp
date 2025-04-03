import pandas as pd
import networkx as nx

def merge_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    print('Merging duplicates started')
    """
    Merge duplicate rows in the DataFrame based on matching on any two of the three keys:
    name, email, or phone. The function normalizes the values (for example, phone numbers
    by removing non-digit characters, and name/email by lowercasing), then uses three keys:
    - key1: name + email
    - key2: name + phone
    - key3: email + phone

    For rows that match on at least one key, the function clusters them using a graph-based approach,
    and merges them into a single row. The 'source' column is updated to include all unique sources
    from the duplicate rows, concatenated with commas.

    Parameters:
        df (pd.DataFrame): The input DataFrame with columns 'name', 'email', 'phone', and 'source'

    Returns:
        pd.DataFrame: A new DataFrame with duplicates merged.
    """

    # Normalize phone numbers (remove non-digit characters)
    df['phone_norm'] = df['phone'].astype(str).str.replace(r'\D', '', regex=True)

    # Normalize name and email (lowercase and strip extra spaces)
    df['name_norm'] = df['name'].str.lower().str.strip()
    df['email_norm'] = df['email'].str.lower().str.strip()

    # Create matching keys for grouping
    df['key1'] = df['name_norm'] + '_' + df['email_norm']
    df['key2'] = df['name_norm'] + '_' + df['phone_norm']
    df['key3'] = df['email_norm'] + '_' + df['phone_norm']

    # Build a graph where each row (index) is a node.
    # If two rows share the same key (key1, key2, or key3), we add an edge between them.
    G = nx.Graph()
    G.add_nodes_from(df.index)

    for key in ['key1', 'key2', 'key3']:
        for group_val, group in df.groupby(key):
            indices = list(group.index)
            if len(indices) > 1:
                # Add edges between all nodes that share this key
                for i in range(len(indices)):
                    for j in range(i + 1, len(indices)):
                        G.add_edge(indices[i], indices[j])

    # Find connected components in the graph;
    # each component represents a cluster of duplicate rows.
    components = list(nx.connected_components(G))

    merged_rows = []
    for comp in components:
        comp_data = df.loc[list(comp)]
        # Select the first row as the base row (can be customized)
        base_row = comp_data.iloc[0].copy()
        # Combine the unique source values from all rows in the component
        sources = comp_data['source'].unique()
        # base_row['source'] = ','.join(sources)
        base_row['source'] = ','.join(str(s) for s in sources if pd.notna(s))
        # You can merge other columns as needed here.
        merged_rows.append(base_row)

    # Create a DataFrame from the merged rows
    merged_df = pd.DataFrame(merged_rows)

    # Optionally, drop helper columns used for matching
    columns_to_drop = ['phone_norm', 'name_norm', 'email_norm', 'key1', 'key2', 'key3']
    merged_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')
    print('Merging duplicates completed')

    return merged_df


# df = pd.read_csv('merged_indeed_linkedin_calendly_us_data.csv')
# merged_df = merge_duplicates(df)
# merged_df.to_csv('merged_output_india.csv', index=False)