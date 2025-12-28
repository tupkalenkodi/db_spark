import subprocess
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


def setup_output_directory(output_dir):
    if output_dir.exists():
        import shutil
        shutil.rmtree(output_dir)

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    return output_dir


def decode_graph6(n: int, graph6_str: str):
    s = graph6_str[1:]
    total_bits = n * (n - 1) // 2

    all_bits = []
    for ch in s:
        c = ord(ch) - 63
        all_bits.extend([(c >> (5 - i)) & 1 for i in range(6)])

    bits = all_bits[:total_bits]
    edges = []
    bit_idx = 0

    for j in range(1, n):
        for i in range(j):
            if bit_idx < len(bits):
                if bits[bit_idx] == 1:
                    edges.append([int(i), int(j)])
            else:
                break
            bit_idx += 1
        if bit_idx >= len(bits):
            break

    return edges


def process_graphs_batch(n, graph6_lines):
    batch_data = {
        'graph6': [],
        'graph_order': [],
        'edges': []
    }

    for g6 in graph6_lines:
        g6 = g6.strip()
        if not g6:
            continue

        edges = decode_graph6(n, g6)

        # Convert edges to string format
        edges_str = ";".join([f"{a},{b}" for a, b in edges])

        batch_data['graph6'].append(g6)
        batch_data['graph_order'].append(n)
        batch_data['edges'].append(edges_str)

    return batch_data


def write_parquet_direct(batch_data, output_file):
    schema = pa.schema([
        ('graph6', pa.string()),
        ('graph_order', pa.int32()),
        ('edges', pa.string())
    ])

    table = pa.table(batch_data, schema=schema)
    pq.write_table(table, str(output_file), compression='none')


def generate_graphs(n: int, output_dir: Path, batch_size):
    min_edges = n - 1
    max_edges = (n * (n + 2)) // 8
    max_degree = n // 2

    cmd = [
        "nauty-geng", str(n),
        "-c",
        f"{min_edges}:{max_edges}",
        "-d1", f"-D{max_degree}",
        "-l"
    ]

    batch_num = 0
    batch_lines = []

    print('\n')
    print('*' * 70)

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, bufsize=1024 * 1024)

    total_graphs = 0
    for line in process.stdout:
        line = line.strip()
        if not line:
            continue

        batch_lines.append(line)
        total_graphs += 1

        # WRITE BATCH WHEN FULL
        if len(batch_lines) >= batch_size:
            # Process all graphs in batch at once
            batch_data = process_graphs_batch(n, batch_lines)

            # Write directly to Parquet
            output_file = output_dir / f"batch_{batch_num:04d}.parquet"
            write_parquet_direct(batch_data, output_file)

            print(f"  Batch {batch_num}: {total_graphs:,} graphs processed...")

            batch_num += 1
            batch_lines = []

    # WRITE FINAL BATCH
    if batch_lines:
        batch_data = process_graphs_batch(n, batch_lines)
        output_file = output_dir / f"batch_{batch_num:04d}.parquet"
        write_parquet_direct(batch_data, output_file)

    print('*' * 70)
    process.wait()

    print('\n')
    print(f"n = {n} -- saved {total_graphs:,} graphs in {batch_num + 1} batches\n")


def main():
    n = 10

    # Get the script's directory (where this Python file is located)
    script_dir = Path(__file__).parent

    # Directory at the project root:
    project_root = script_dir.parent
    output_dir = project_root / "data" / "generated" / f"order={n}"

    # Setup output directory
    output_dir = setup_output_directory(output_dir)

    print("=" * 70)
    print(f"PATTERN GRAPH SEARCH SPACE GENERATION")
    print("=" * 70)

    generate_graphs(n, output_dir, batch_size=1000000)

    print("=" * 70)
    print("GENERATION COMPLETE!")
    print("=" * 70)


if __name__ == "__main__":
    main()