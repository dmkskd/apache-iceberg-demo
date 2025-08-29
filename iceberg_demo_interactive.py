# COMPREHENSIVE APACHE ICEBERG DEMONSTRATION - RICH TUI VERSION
# ==============================================================
# This script demonstrates Iceberg's ACID operations using Rich TUI for better readability

import os
import shutil
import time
import json
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, MapType
from pyiceberg import expressions
import argparse

# Rich imports for TUI
from rich.console import Console
from rich.panel import Panel
from rich.tree import Tree
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
from rich.rule import Rule


class IcebergTUI:
    def __init__(self, test_mode=False):
        self.console = Console()
        self.warehouse_path = "local_warehouse"
        self.catalog = None
        self.table = None
        self.snapshots = []
        self.current_step = 0
        self.total_steps = 7
        self.test_mode = test_mode
        self.layout = self.make_layout()

    def make_layout(self):
        """Defines the layout of the TUI."""
        layout = Layout(name="root")
        layout.split(
            Layout(size=1, name="header"),
            Layout(size=65, name="main"), # Fixed height for main
            Layout(size=5, name="footer")
        )
        layout["main"].split_row(
            Layout(name="left", ratio=1), # Left column takes half of main width
            Layout(name="files", ratio=1) # Files column takes half of main width
        )
        # Now, split the left column into content and data with fixed sizes
        # Total for left is 65 lines.
        # Content is usually shorter, data can be longer.
        layout["left"].split_column(
            Layout(size=20, name="content"), # Fixed height for content
            Layout(size=45, name="data")     # Fixed height for data
        )

        layout["footer"].split_row(
            Layout(name="progress"),
            Layout(name="status")
        )
        return layout

    def create_progress_panel(self):
        """Create compact progress panel"""
        progress_text = f"Step {self.current_step}/{self.total_steps}"

        steps = ["Setup", "Insert", "Upsert", "Delete", "Schema", "Travel", "Analysis"]

        step_status = ""
        for i, step in enumerate(steps, 1):
            if i < self.current_step:
                step_status += f"✅ {i}. {step}  "
            elif i == self.current_step:
                step_status += f"👉 {i}. {step}  "
            else:
                step_status += f"⏳ {i}. {step}  "

        return Panel(
            f"{progress_text}\n{step_status}",
            title="📊 Progress",
            border_style="blue",
            title_align="left"
        )

    def create_status_panel(self, message="Ready"):
        """Create status panel with current instructions"""
        if self.current_step == 0:
            instructions = "👋 Welcome! Get ready to explore Apache Iceberg\n👉 Press ENTER to start"
        else:
            instructions = "👉 Press ENTER to continue to the next step"

        return Panel(
            f"Status: {message}\n{instructions}",
            title="ℹ️ Status",
            border_style="green",
            title_align="left"
        )

    def create_content_panel(self, title, content):
        """Create main content panel"""
        return Panel(
            content,
            title=f"🎯 {title}",
            border_style="cyan",
            title_align="left"
        )

    def create_data_panel(self, df, title="📊 Data"):
        """Create panel for displaying DataFrame"""
        if isinstance(df, pd.DataFrame) and df.empty:
            content = "No data to display"
        elif isinstance(df, str):
            content = df
        else:
            # Create a Rich table
            table = Table(show_header=True, header_style="bold blue")

            # Add columns
            for col in df.columns:
                table.add_column(col)

            # Add rows (limit to first 10 for display)
            for _, row in df.head(10).iterrows():
                table.add_row(*[str(val) for val in row])

            if len(df) > 10:
                table.add_row(*["..." for _ in df.columns])
                table.add_row(*[f"({len(df)} total rows)" if i == 0 else "" for i in range(len(df.columns))])

            content = table

        return Panel(
            content,
            title=title,
            border_style="yellow",
            title_align="left"
        )

    def create_file_tree_panel(self):
        """Create a summarized file tree panel to keep it compact."""
        if not os.path.exists(self.warehouse_path):
            return Panel("Warehouse not yet created", title="📁 Files", border_style="red")

        tree = Tree("🏪 local_warehouse")

        for root, dirs, files in os.walk(self.warehouse_path):
            rel_path = os.path.relpath(root, self.warehouse_path)
            current_node = tree
            if rel_path != ".":
                path_parts = rel_path.split(os.sep)
                for part in path_parts:
                    found = False
                    for child in current_node.children:
                        if child.label.endswith(part):
                            current_node = child
                            found = True
                            break
                    if not found:
                        current_node = current_node.add(f"📁 {part}")

            # Summarize data and metadata directories
            if os.path.basename(root) == 'data':
                parquet_files = [f for f in files if f.endswith('.parquet')]
                if parquet_files:
                    current_node.add(f"🗄️ ({len(parquet_files)} Parquet files)")
                dirs[:] = []  # Don't descend further
                continue
            elif os.path.basename(root) == 'metadata':
                json_files = sorted([f for f in files if f.endswith('.metadata.json')])
                manifest_lists = [f for f in files if f.startswith('snap-') and f.endswith('.avro')]
                manifests = [f for f in files if '-m' in f and f.endswith('.avro')]

                if json_files:
                    latest_json = json_files[-1]
                    file_size = os.path.getsize(os.path.join(root, latest_json))
                    current_node.add(f"📋 [bold]{latest_json}[/] ({file_size:,}b)")
                    if len(json_files) > 1:
                        current_node.add(f"📋 ({len(json_files) - 1} older metadata files)")
                if manifest_lists:
                    current_node.add(f"📊 ({len(manifest_lists)} manifest lists)")
                if manifests:
                    current_node.add(f"📊 ({len(manifests)} manifest files)")
                dirs[:] = [] # Don't descend further
                continue

            # Add other files as normal
            for file in sorted(files):
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                current_node.add(f"📄 {file} ({file_size:,}b)")

        return Panel(tree, title="📁 Files", border_style="green")

    def wait_for_input(self, message="Press ENTER to continue..."):
        """Wait for user input or sleep in test mode"""
        if self.test_mode:
            time.sleep(1)
            return
        try:
            self.console.input(f"[bold green] {message} [/]")
        except KeyboardInterrupt:
            self.console.print("\n👋 Goodbye!")
            exit()

    def cleanup_warehouse(self):
        """Clean up warehouse directory"""
        if os.path.exists(self.warehouse_path):
            shutil.rmtree(self.warehouse_path)

    def run(self):
        """Run the complete demonstration using a live layout."""
        with open("debug_height.log", "w") as f:
            f.write(f"Console height: {self.console.height}\n")
        try:
            with Live(self.layout, screen=True, redirect_stderr=False):
                self.layout["header"].update(Rule("[bold blue]Apache Iceberg TUI Demo[/]"))
                # Step 1: Setup
                self.current_step = 1
                content = """🚀 ICEBERG ENVIRONMENT SETUP

We'll create a local Iceberg catalog using SQLite and define our table schema.

This establishes our foundation for demonstrating ACID operations."""
                self.layout["content"].update(self.create_content_panel("Step 1: Environment Setup", content))
                self.layout["data"].update(self.create_data_panel(pd.DataFrame(), title="📋 Info"))
                self.layout["files"].update(self.create_file_tree_panel())
                self.layout["progress"].update(self.create_progress_panel())
                self.layout["status"].update(self.create_status_panel("Ready to setup environment"))
                self.wait_for_input()

                self.cleanup_warehouse()
                os.makedirs(self.warehouse_path, exist_ok=True)
                catalog_config = {
                    "uri": f"sqlite:///{self.warehouse_path}/catalog.db",
                    "warehouse": f"file://{os.path.abspath(self.warehouse_path)}"
                }
                self.catalog = load_catalog("default", **catalog_config)
                schema = Schema(
                    NestedField(1, "id", LongType(), required=True),
                    NestedField(2, "name", StringType(), required=True),
                    NestedField(3, "metadata", MapType(key_id=4, key_type=StringType(), value_id=5, value_type=StringType()), required=False)
                )
                self.catalog.create_namespace("test_db")
                self.table = self.catalog.create_table("test_db.users", schema)

                completion_info = """✅ Environment setup complete!

Created:
• SQLite catalog at: local_warehouse/catalog.db
• Table: test_db.users
• Schema: id (Long), name (String), metadata (Map<String,String>)

Ready for data operations!"""
                self.layout["data"].update(self.create_data_panel(completion_info, title="✅ Setup Complete"))
                self.layout["files"].update(self.create_file_tree_panel())
                self.layout["status"].update(self.create_status_panel("Setup complete"))
                self.wait_for_input()

                # Step 2: Insert
                self.current_step = 2
                content = """📥 INITIAL DATA INSERT (V1)

We'll insert 4 users into our empty table. This creates the first snapshot."""
                data_v1 = {
                    "id": [1, 2, 3, 4],
                    "name": ["Alice", "Bob", "Charlie", "Diana"],
                    "metadata": [
                        {"city": "New York", "dept": "Engineering"},
                        {"city": "San Francisco", "dept": "Marketing"},
                        {"city": "London", "dept": "Sales"},
                        {"city": "Toronto", "dept": "Engineering"}
                    ]
                }
                df_v1 = pd.DataFrame(data_v1)
                self.layout["content"].update(self.create_content_panel("Step 2: Initial Insert", content))
                self.layout["data"].update(self.create_data_panel(df_v1, "📊 Data to Insert"))
                self.layout["progress"].update(self.create_progress_panel())
                self.layout["status"].update(self.create_status_panel("Ready to insert initial data"))
                self.wait_for_input()

                pa_table_v1 = pa.Table.from_pandas(df_v1, schema=self.table.schema().as_arrow(), preserve_index=False)
                self.table.append(pa_table_v1)
                snapshot_id = self.table.current_snapshot().snapshot_id
                self.snapshots.append(snapshot_id)

                result_df = self.table.scan().to_pandas()
                self.layout["data"].update(self.create_data_panel(result_df, f"✅ V1 Data (Snapshot: {snapshot_id})"))
                self.layout["files"].update(self.create_file_tree_panel())
                self.layout["status"].update(self.create_status_panel("Insert complete"))
                self.wait_for_input()

                # Step 3: Upsert
                self.current_step = 3
                content = """🔄 UPSERT OPERATION (V2)

We'll update some existing records and add new ones. Iceberg handles this as an atomic operation."""
                self.layout["content"].update(self.create_content_panel("Step 3: Upsert Operation", content))
                current_df = self.table.scan().to_pandas()
                self.layout["data"].update(self.create_data_panel(current_df, "📊 Current Data"))
                self.layout["progress"].update(self.create_progress_panel())
                self.layout["status"].update(self.create_status_panel("Ready to perform upsert"))
                self.wait_for_input()

                data_v2 = {
                    "id": [2, 4, 5, 6],
                    "name": ["Robert", "Diana", "Eve", "Frank"],
                    "metadata": [
                        {"city": "San Francisco", "dept": "Engineering"},
                        {"city": "Toronto", "dept": "Marketing"},
                        {"city": "Paris", "dept": "Engineering"},
                        {"city": "Berlin", "dept": "Sales"}
                    ]
                }
                df_v2 = pd.DataFrame(data_v2)
                pa_table_v2 = pa.Table.from_pandas(df_v2, schema=self.table.schema().as_arrow(), preserve_index=False)
                self.table.upsert(pa_table_v2, join_cols=['id'])
                snapshot_id = self.table.current_snapshot().snapshot_id
                self.snapshots.append(snapshot_id)

                result_df = self.table.scan().to_pandas()
                self.layout["data"].update(self.create_data_panel(result_df, f"✅ V2 Data (Snapshot: {snapshot_id})"))
                self.layout["files"].update(self.create_file_tree_panel())
                self.layout["status"].update(self.create_status_panel("Upsert complete"))
                self.wait_for_input()

                # Step 4: Delete
                self.current_step = 4
                content = """🗑️ DELETE OPERATION (V3)

We'll delete specific records based on criteria. This preserves data history."""
                self.layout["content"].update(self.create_content_panel("Step 4: Delete Operation", content))
                current_df = self.table.scan().to_pandas()
                self.layout["data"].update(self.create_data_panel(current_df, "📊 Data Before Delete"))
                self.layout["progress"].update(self.create_progress_panel())
                self.layout["status"].update(self.create_status_panel("Ready to perform delete"))
                self.wait_for_input()

                self.table.delete(expressions.GreaterThan("id", 3))
                snapshot_id = self.table.current_snapshot().snapshot_id
                self.snapshots.append(snapshot_id)

                result_df = self.table.scan().to_pandas()
                self.layout["data"].update(self.create_data_panel(result_df, f"✅ V3 Data (Snapshot: {snapshot_id})"))
                self.layout["files"].update(self.create_file_tree_panel())
                self.layout["status"].update(self.create_status_panel("Delete complete"))
                self.wait_for_input()

                # Step 5: Schema
                self.current_step = 5
                content = """📋 SCHEMA EVOLUTION

Let's examine how Iceberg manages schema changes and data structure."""
                schema_info = f"""Current Schema:
• ID: {self.table.schema().schema_id}
• Fields: {len(self.table.schema().fields)} total

Field Details:
1. id: {self.table.schema().find_field('id').field_type} (required)
2. name: {self.table.schema().find_field('name').field_type} (required)
3. metadata: {self.table.schema().find_field('metadata').field_type} (optional)

Iceberg tracks schema evolution across all snapshots."""
                self.layout["content"].update(self.create_content_panel("Step 5: Schema Analysis", content))
                self.layout["data"].update(self.create_data_panel(schema_info, title="📋 Schema Info"))
                self.layout["progress"].update(self.create_progress_panel())
                self.layout["status"].update(self.create_status_panel("Analyzing schema structure"))
                self.wait_for_input()

                # Step 6: Time Travel
                self.current_step = 6
                content = """🕰️ TIME TRAVEL DEMONSTRATION

Iceberg's time travel lets us query historical versions of our data."""
                self.layout["content"].update(self.create_content_panel("Step 6: Time Travel", content))
                self.layout["progress"].update(self.create_progress_panel())

                for i, snapshot_id in enumerate(self.snapshots, 1):
                    historical_df = self.table.scan(snapshot_id=snapshot_id).to_pandas()
                    self.layout["data"].update(self.create_data_panel(
                        historical_df,
                        f"🕰️ V{i} (Snapshot: {snapshot_id}) - {len(historical_df)} records"
                    ))
                    self.layout["status"].update(self.create_status_panel(f"Showing snapshot {i}"))
                    self.wait_for_input()

                # Step 7: Analysis
                self.current_step = 7
                content = """🔍 METADATA ANALYSIS

Let's dive deep into Iceberg's metadata files to understand how it tracks changes."""
                metadata_dir = f"{self.warehouse_path}/test_db/users/metadata"
                analysis = ""
                if os.path.exists(metadata_dir):
                    metadata_files = [f for f in os.listdir(metadata_dir) if f.endswith('.metadata.json')]
                    metadata_files.sort()
                    if metadata_files:
                        latest_metadata = metadata_files[-1]
                        metadata_path = os.path.join(metadata_dir, latest_metadata)
                        try:
                            with open(metadata_path, 'r') as f:
                                metadata = json.load(f)
                            analysis = f"""Metadata Analysis:

Files found: {len(metadata_files)} metadata.json files

Latest metadata ({latest_metadata}):
• Format version: {metadata.get('format-version', 'Unknown')}
• Table UUID: {metadata.get('table-uuid', 'Unknown')[:8]}...
• Current schema ID: {metadata.get('current-schema-id', 'Unknown')}
• Current snapshot: {metadata.get('current-snapshot-id', 'Unknown')}

Snapshots tracked: {len(metadata.get('snapshots', []))}

This metadata enables ACID guarantees and time travel!"""
                        except Exception as e:
                            analysis = f"Error reading metadata: {e}"
                    else:
                        analysis = "No metadata files found"
                else:
                    analysis = "Metadata directory not found"
                self.layout["content"].update(self.create_content_panel("Step 7: Metadata Analysis", content))
                self.layout["data"].update(self.create_data_panel(analysis, title="🔍 Metadata Analysis"))
                self.layout["progress"].update(self.create_progress_panel())
                self.layout["status"].update(self.create_status_panel("Analyzing metadata structure"))
                self.wait_for_input()

                # Completion
                self.current_step = self.total_steps
                content = """🎉 DEMONSTRATION COMPLETE!

You've successfully explored:
✅ ACID operations (Insert, Upsert, Delete)
✅ File structure and organization
✅ Time travel capabilities
✅ Metadata and snapshot evolution
✅ Schema management

Key Takeaway:
Apache Iceberg provides enterprise-grade ACID guarantees while maintaining complete data lineage and the ability to query any historical version of your data!

Perfect for data lakes that need reliability! 🚀"""
                self.layout["content"].update(self.create_content_panel("🎉 Complete!", content))
                self.layout["data"].update(self.create_data_panel("Demo finished successfully! 🎊", title="✅ Status"))
                self.layout["progress"].update(self.create_progress_panel())
                self.layout["status"].update(self.create_status_panel("Thank you for exploring Apache Iceberg!"))
                self.wait_for_input()

        except KeyboardInterrupt:
            self.console.print("\n👋 Demo interrupted. Goodbye!")
        except Exception as e:
            self.console.print(f"\n❌ Error during demo: {e}")
            import traceback
            traceback.print_exc()


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Apache Iceberg Interactive Demo.")
    parser.add_argument("--test-mode", action="store_true", help="Run in non-interactive test mode.")
    args = parser.parse_args()

    demo = IcebergTUI(test_mode=args.test_mode)
    demo.run()


if __name__ == "__main__":
    main()