# COMPREHENSIVE APACHE ICEBERG DEMONSTRATION - RICH TUI VERSION
# ==============================================================
# This script demonstrates Iceberg's ACID operations using Rich TUI for better readability

import os
import shutil
import time
import json
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, MapType
from pyiceberg.io import load_file_io
import fastavro
from pyiceberg import expressions

# Rich imports for TUI
from rich.console import Console
from rich.panel import Panel
from rich.columns import Columns
from rich.tree import Tree
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.align import Align
from rich.rule import Rule
import json


class IcebergTUI:
    def __init__(self):
        self.console = Console()
        self.warehouse_path = "local_warehouse"
        self.catalog = None
        self.table = None
        self.snapshots = []
        self.current_step = 0
        self.total_steps = 7
        
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
        if df.empty:
            content = "No data to display"
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
        """Create file tree panel"""
        if not os.path.exists(self.warehouse_path):
            return Panel("Warehouse not yet created", title="📁 Files", border_style="red")
        
        tree = Tree("🏪 local_warehouse")
        
        for root, dirs, files in os.walk(self.warehouse_path):
            # Get relative path
            rel_path = os.path.relpath(root, self.warehouse_path)
            
            # Find the current node in tree
            current_node = tree
            if rel_path != ".":
                path_parts = rel_path.split(os.sep)
                for part in path_parts:
                    # Find or create node for this part
                    found = False
                    for child in current_node.children:
                        if child.label.plain.endswith(part):
                            current_node = child
                            found = True
                            break
                    if not found:
                        current_node = current_node.add(f"📁 {part}")
            
            # Add files to current node
            for file in sorted(files):
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                if file.endswith('.parquet'):
                    icon = "🗄️"
                elif file.endswith('.json'):
                    icon = "📋"
                elif file.endswith('.avro'):
                    icon = "📊"
                else:
                    icon = "📄"
                current_node.add(f"{icon} {file} ({file_size:,}b)")
        
        return Panel(tree, title="📁 Files", border_style="green")
    
    def create_layout(self):
        """Create main layout"""
        layout = Layout()
        
        # Split into top and bottom
        layout.split_column(
            Layout(name="main", ratio=4),
            Layout(name="bottom", ratio=1)
        )
        
        # Split main into left (2/3) and right (1/3)
        layout["main"].split_row(
            Layout(name="content", ratio=2),
            Layout(name="sidebar", ratio=1)
        )
        
        # Split content into top and bottom
        layout["content"].split_column(
            Layout(name="primary", ratio=2),
            Layout(name="data", ratio=1)
        )
        
        # Split sidebar into progress and files
        layout["sidebar"].split_column(
            Layout(name="progress", ratio=1),
            Layout(name="files", ratio=2)
        )
        
        return layout
    
    def wait_for_input(self, message="Press ENTER to continue..."):
        """Wait for user input"""
        try:
            input()
        except KeyboardInterrupt:
            self.console.print("\n👋 Goodbye!")
            exit()
    
    def step_1_setup(self):
        """Step 1: Environment Setup"""
        self.current_step = 1
        
        content = """🚀 ICEBERG ENVIRONMENT SETUP

We'll create a local Iceberg catalog using SQLite and define our table schema.

What we're about to do:
• Clean up any existing warehouse
• Create a new catalog  
• Define a users table with id, name, and metadata columns

This establishes our foundation for demonstrating ACID operations."""
        
        layout = self.create_layout()
        layout["primary"] = self.create_content_panel("Step 1: Environment Setup", content)
        layout["data"] = Panel("Environment setup in progress...", title="📋 Info", border_style="yellow")
        layout["progress"] = self.create_progress_panel()
        layout["files"] = self.create_file_tree_panel()
        layout["bottom"] = self.create_status_panel("Ready to setup environment")
        
        with Live(layout, refresh_per_second=4):
            self.wait_for_input()
        
        # Perform setup
        self.cleanup_warehouse()
        catalog_config = {
            "uri": f"sqlite:///{self.warehouse_path}/catalog.db",
            "warehouse": f"file://{os.path.abspath(self.warehouse_path)}"
        }
        
        self.catalog = load_catalog("default", **catalog_config)
        
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "name", StringType(), required=True),
            NestedField(3, "metadata", MapType(StringType(), StringType()), required=False)
        )
        
        self.table = self.catalog.create_table("test_db.users", schema)
        
        # Show completion
        completion_info = """✅ Environment setup complete!

Created:
• SQLite catalog at: local_warehouse/catalog.db
• Table: test_db.users
• Schema: id (Long), name (String), metadata (Map<String,String>)

Ready for data operations!"""
        
        layout["data"] = Panel(completion_info, title="✅ Setup Complete", border_style="green")
        layout["files"] = self.create_file_tree_panel()
        
        with Live(layout, refresh_per_second=4):
            time.sleep(2)
    
    def step_2_insert(self):
        """Step 2: Initial Data Insert"""
        self.current_step = 2
        
        content = """📥 INITIAL DATA INSERT (V1)

We'll insert 4 users into our empty table. This creates the first snapshot.

What we're about to do:
• Insert 4 users: Alice, Bob, Charlie, Diana
• Create first Parquet file
• Generate manifest files  
• Update metadata.json

This demonstrates Iceberg's atomic write operations."""
        
        layout = self.create_layout()
        layout["primary"] = self.create_content_panel("Step 2: Initial Insert", content)
        layout["progress"] = self.create_progress_panel()
        layout["files"] = self.create_file_tree_panel()
        layout["bottom"] = self.create_status_panel("Ready to insert initial data")
        
        # Prepare data
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
        layout["data"] = self.create_data_panel(df_v1, "📊 Data to Insert")
        
        with Live(layout, refresh_per_second=4):
            self.wait_for_input()
        
        # Perform insert
        pa_table_v1 = pa.Table.from_pandas(df_v1, schema=self.table.schema().as_arrow(), preserve_index=False)
        self.table.append(pa_table_v1)
        
        snapshot_id = self.table.current_snapshot().snapshot_id
        self.snapshots.append(snapshot_id)
        
        # Show result
        result_df = self.table.scan().to_pandas()
        layout["data"] = self.create_data_panel(result_df, f"✅ V1 Data (Snapshot: {snapshot_id})")
        layout["files"] = self.create_file_tree_panel()
        
        with Live(layout, refresh_per_second=4):
            time.sleep(2)
    
    def step_3_upsert(self):
        """Step 3: Upsert Operation"""
        self.current_step = 3
        
        content = """🔄 UPSERT OPERATION (V2)

We'll update some existing records and add new ones. Iceberg handles this as an atomic operation.

What we're about to do:
• Update Bob → Robert (promotion)
• Update Diana's department
• Add Eve and Frank
• Result: 6 total users

This shows Iceberg's merge capabilities."""
        
        layout = self.create_layout()
        layout["primary"] = self.create_content_panel("Step 3: Upsert Operation", content)
        layout["progress"] = self.create_progress_panel()
        layout["files"] = self.create_file_tree_panel()
        layout["bottom"] = self.create_status_panel("Ready to perform upsert")
        
        # Show current data first
        current_df = self.table.scan().to_pandas()
        layout["data"] = self.create_data_panel(current_df, "📊 Current Data")
        
        with Live(layout, refresh_per_second=4):
            self.wait_for_input()
        
        # Prepare upsert data
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
        layout["data"] = self.create_data_panel(df_v2, "📊 Upsert Data")
        
        with Live(layout, refresh_per_second=4):
            time.sleep(1)
        
        # Perform upsert
        pa_table_v2 = pa.Table.from_pandas(df_v2, schema=self.table.schema().as_arrow(), preserve_index=False)
        self.table.overwrite(pa_table_v2)
        
        snapshot_id = self.table.current_snapshot().snapshot_id
        self.snapshots.append(snapshot_id)
        
        # Show result
        result_df = self.table.scan().to_pandas()
        layout["data"] = self.create_data_panel(result_df, f"✅ V2 Data (Snapshot: {snapshot_id})")
        layout["files"] = self.create_file_tree_panel()
        
        with Live(layout, refresh_per_second=4):
            time.sleep(2)
    
    def step_4_delete(self):
        """Step 4: Delete Operation"""
        self.current_step = 4
        
        content = """🗑️ DELETE OPERATION (V3)

We'll delete specific records based on criteria. This preserves data history.

What we're about to do:
• Delete users with id > 3 (Diana, Eve, Frank)
• Keep Alice, Charlie, Robert
• Result: 3 remaining users

This demonstrates Iceberg's selective deletion while maintaining history."""
        
        layout = self.create_layout()
        layout["primary"] = self.create_content_panel("Step 4: Delete Operation", content)
        layout["progress"] = self.create_progress_panel()
        layout["files"] = self.create_file_tree_panel()
        layout["bottom"] = self.create_status_panel("Ready to perform delete")
        
        # Show current data
        current_df = self.table.scan().to_pandas()
        layout["data"] = self.create_data_panel(current_df, "📊 Data Before Delete")
        
        with Live(layout, refresh_per_second=4):
            self.wait_for_input()
        
        # Perform delete
        self.table.delete(expressions.GreaterThan("id", 3))
        
        snapshot_id = self.table.current_snapshot().snapshot_id
        self.snapshots.append(snapshot_id)
        
        # Show result
        result_df = self.table.scan().to_pandas()
        layout["data"] = self.create_data_panel(result_df, f"✅ V3 Data (Snapshot: {snapshot_id})")
        layout["files"] = self.create_file_tree_panel()
        
        with Live(layout, refresh_per_second=4):
            time.sleep(2)
    
    def step_5_schema(self):
        """Step 5: Schema Evolution"""
        self.current_step = 5
        
        content = """📋 SCHEMA EVOLUTION

Let's examine how Iceberg manages schema changes and data structure.

What we're exploring:
• Table schema definition
• Column types and constraints
• Nested data structures (metadata map)
• Schema versioning

This shows Iceberg's flexible schema management."""
        
        layout = self.create_layout()
        layout["primary"] = self.create_content_panel("Step 5: Schema Analysis", content)
        layout["progress"] = self.create_progress_panel()
        layout["files"] = self.create_file_tree_panel()
        layout["bottom"] = self.create_status_panel("Analyzing schema structure")
        
        # Show schema information
        schema_info = f"""Current Schema:
• ID: {self.table.schema().schema_id}
• Fields: {len(self.table.schema().fields)} total

Field Details:
1. id: {self.table.schema().find_field('id').field_type} (required)
2. name: {self.table.schema().find_field('name').field_type} (required)  
3. metadata: {self.table.schema().find_field('metadata').field_type} (optional)

Iceberg tracks schema evolution across all snapshots."""
        
        layout["data"] = Panel(schema_info, title="📋 Schema Info", border_style="cyan")
        
        with Live(layout, refresh_per_second=4):
            self.wait_for_input()
    
    def step_6_time_travel(self):
        """Step 6: Time Travel"""
        self.current_step = 6
        
        content = """🕰️ TIME TRAVEL DEMONSTRATION

Iceberg's time travel lets us query historical versions of our data.

What we're exploring:
• Query V1: Original 4 users
• Query V2: After upsert (6 users)
• Query V3: After delete (3 users)
• Show how each snapshot represents a point in time

This is Iceberg's killer feature - complete data lineage!"""
        
        layout = self.create_layout()
        layout["primary"] = self.create_content_panel("Step 6: Time Travel", content)
        layout["progress"] = self.create_progress_panel()
        layout["files"] = self.create_file_tree_panel()
        layout["bottom"] = self.create_status_panel("Ready to explore time travel")
        
        with Live(layout, refresh_per_second=4):
            self.wait_for_input()
        
        # Show each version
        for i, snapshot_id in enumerate(self.snapshots, 1):
            historical_df = self.table.scan().use_ref(str(snapshot_id)).to_pandas()
            
            layout["data"] = self.create_data_panel(
                historical_df, 
                f"🕰️ V{i} (Snapshot: {snapshot_id}) - {len(historical_df)} records"
            )
            
            with Live(layout, refresh_per_second=4):
                time.sleep(2)
    
    def step_7_analysis(self):
        """Step 7: Metadata Analysis"""
        self.current_step = 7
        
        content = """🔍 METADATA ANALYSIS

Let's dive deep into Iceberg's metadata files to understand how it tracks changes.

What we're exploring:
• metadata.json files evolution
• Manifest lists and files
• Snapshot relationships
• File organization

This reveals Iceberg's internal architecture."""
        
        layout = self.create_layout()
        layout["primary"] = self.create_content_panel("Step 7: Metadata Analysis", content)
        layout["progress"] = self.create_progress_panel()
        layout["files"] = self.create_file_tree_panel()
        layout["bottom"] = self.create_status_panel("Analyzing metadata structure")
        
        # Analyze metadata
        metadata_dir = f"{self.warehouse_path}/test_db/users/metadata"
        
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
        
        layout["data"] = Panel(analysis, title="🔍 Metadata Analysis", border_style="magenta")
        
        with Live(layout, refresh_per_second=4):
            self.wait_for_input()
        
        # Final summary
        self.show_completion()
    
    def show_completion(self):
        """Show completion screen"""
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
        
        layout = self.create_layout()
        layout["primary"] = self.create_content_panel("🎉 Complete!", content)
        layout["data"] = Panel("Demo finished successfully! 🎊", title="✅ Status", border_style="green")
        layout["progress"] = self.create_progress_panel()
        layout["files"] = self.create_file_tree_panel()
        layout["bottom"] = Panel("🙏 Thank you for exploring Apache Iceberg!", title="👋 Goodbye", border_style="green")
        
        with Live(layout, refresh_per_second=4):
            time.sleep(3)
    
    def cleanup_warehouse(self):
        """Clean up warehouse directory"""
        if os.path.exists(self.warehouse_path):
            shutil.rmtree(self.warehouse_path)
    
    def run(self):
        """Run the complete demonstration"""
        self.console.print("🚀 [bold blue]Apache Iceberg Interactive Demo[/bold blue]")
        self.console.print("Welcome to the comprehensive Iceberg demonstration!")
        self.console.print()
        
        try:
            self.step_1_setup()
            self.step_2_insert()
            self.step_3_upsert()
            self.step_4_delete()
            self.step_5_schema()
            self.step_6_time_travel()
            self.step_7_analysis()
            
        except KeyboardInterrupt:
            self.console.print("\n👋 Demo interrupted. Goodbye!")
        except Exception as e:
            self.console.print(f"\n❌ Error during demo: {e}")
            import traceback
            traceback.print_exc()


def main():
    """Main function"""
    demo = IcebergTUI()
    demo.run()


if __name__ == "__main__":
    main()
