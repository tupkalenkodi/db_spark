import networkx as nx
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import numpy as np
import random
import shutil
import matplotlib.pyplot as plt


def visualize_graph(graph, output_dir):
    plt.figure(figsize=(12, 10))

    if graph.number_of_nodes() < 100:
        pos = nx.spring_layout(graph, k=0.5, iterations=50, seed=42)
    else:
        pos = nx.kamada_kawai_layout(graph)

    nx.draw_networkx_nodes(graph, pos,
                           node_color='lightblue',
                           node_size=500,
                           alpha=0.9)

    nx.draw_networkx_edges(graph, pos,
                           width=1.5,
                           alpha=0.6,
                           edge_color='gray')

    nx.draw_networkx_labels(graph, pos,
                            font_size=10,
                            font_weight='bold')

    plt.title(f"Graph with {graph.number_of_nodes()} nodes and {graph.number_of_edges()} edges")
    plt.axis('off')
    plt.tight_layout()

    plt.show()

def setup_output_directory(output_dir):
    if output_dir.exists():
        shutil.rmtree(output_dir)

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    return output_dir


def generate_graph(num_components, component_sizes, graph_type, **kwargs):

    sizes = []
    (m, s) = component_sizes
    for _ in range(num_components):
        size = np.random.normal(m, s)
        size = round(size)
        sizes.append(size)

    sizes = [int(size) for size in sizes]
    components = []
    node_offset = 0

    print('\n')
    print('*' * 70)

    for i, size in enumerate(sizes):
        print(f"Generating component {i+1}/{num_components} with {size} nodes...")

        if graph_type == "erdos_renyi":
            p = kwargs.get('p', 0.3)
            graph = nx.erdos_renyi_graph(size, p)
        elif graph_type == "barabasi_albert":
            m = kwargs.get('m', 3)
            graph = nx.barabasi_albert_graph(size, m)
        elif graph_type == "watts_strogatz":
            k = kwargs.get('k', 4)
            p = kwargs.get('p', 0.3)
            graph = nx.watts_strogatz_graph(size, k, p)
        elif graph_type == "random_regular":
            d = kwargs.get('d', 3)
            graph = nx.random_regular_graph(d, size)
        else:
            return None

        mapping = {old: f"{node_offset + old}" for old in graph.nodes()}
        graph = nx.relabel_nodes(graph, mapping)

        components.append(graph)
        node_offset += size

    combined_graph = nx.Graph()
    for component in components:
        combined_graph = nx.compose(combined_graph, component)

    return combined_graph


def print_statistics(graph):
    degrees = [d for n, d in graph.degree()]

    print('\n')
    print("GRAPH STATISTICS")
    print("-" * 70)
    print(f"Nodes: {graph.number_of_nodes()}")
    print(f"Edges: {graph.number_of_edges()}")
    print(f"Connected components: {nx.number_connected_components(graph)}")
    print(f"Average degree: {sum(degrees) / len(degrees):.2f}")
    print(f"Min degree: {min(degrees)}")
    print(f"Max degree: {max(degrees)}")

    print("*" * 70)
    print('\n')


def save_to_parquet(graph, output_dir, vertices_path=None, edges_path=None):
    if vertices_path is None:
        vertices_path = output_dir / "vertices.parquet"
    if edges_path is None:
        edges_path = output_dir / "edges.parquet"

    vertices_data = {
        'id': [node for node in graph.nodes()]
    }

    edges_data = {
        'src': [],
        'dst': []
    }

    for u, v in graph.edges():
        edges_data['src'].append(u)
        edges_data['dst'].append(v)

    vertices_schema = pa.schema([
        ('id', pa.string())
    ])
    vertices_table = pa.table(vertices_data, schema=vertices_schema)
    pq.write_table(vertices_table, str(vertices_path), compression='none')

    edges_schema = pa.schema([
        ('src', pa.string()),
        ('dst', pa.string())
    ])
    edges_table = pa.table(edges_data, schema=edges_schema)
    pq.write_table(edges_table, str(edges_path), compression='none')


def main():
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    output_dir = project_root / "data" / "target" / "large"
    output_dir = setup_output_directory(output_dir)


    num_components = 5
    # Set to [20, 30, 40] for specific sizes, or None for random
    component_sizes_mean_sd = (100, 10)

    # Graph type: 'erdos_renyi', 'barabasi_albert', 'watts_strogatz',
    #             'random_regular', 'powerlaw_cluster', 'complete', 'cycle'
    graph_type = 'barabasi_albert'

    # Graph parameters
    p = 0.3   # Probability parameter (for erdos_renyi, watts_strogatz)
    m = 3     # Edge parameter (for barabasi_albert, powerlaw_cluster)
    k = 6     # Degree parameter (for watts_strogatz)
    d = 4     # Degree for random_regular graphs

    seed = 42
    random.seed(seed)

    print("=" * 70)
    print(f"TARGET GRAPH GENERATION")
    print("=" * 70)

    graph = generate_graph(num_components=num_components,component_sizes=component_sizes_mean_sd,
                           graph_type=graph_type, p=p, m=m, k=k, d=d)

    print("Printing Statistics...")
    print_statistics(graph)

    print("Saving Files...")
    save_to_parquet(graph, output_dir)

    # visualize_graph(graph, output_dir)

    print("=" * 70)
    print("GENERATION COMPLETE!")
    print("=" * 70)

if __name__ == "__main__":
    main()
