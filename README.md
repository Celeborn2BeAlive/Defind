# DefInd 🔍

**DefInd** is a high-performance, Python-native DeFi log fetcher designed for efficient extraction and processing of Ethereum blockchain event data. Built specifically for DeFi protocols, it provides resumable, concurrent data fetching with dynamic column projections and universal storage format.

## ✨ Features

- **🚀 High Performance**: Async/concurrent fetching with configurable parallelism
- **🔄 Resumable Operations**: Live manifest system for fault-tolerant data collection
- **📊 Dynamic Projections**: Any registry key automatically becomes a column
- **💾 Universal Storage**: Efficient Parquet-based sharding with bounded memory
- **🎯 DeFi-Optimized**: Pre-built support for CL pools, Gauge, and VFAT events
- **⚡ Smart Filtering**: Fast zero-word filtering and automatic interval splitting
- **🛡️ Robust Error Handling**: Automatic retries and graceful failure recovery

## 🏗️ Architecture

```
defind/
├── core/           # Data models and constants
├── clients/        # RPC client for Ethereum nodes
├── decoding/       # Event decoding with dynamic projections
├── storage/        # Universal shards and live manifests
└── orchestration/  # Streaming fetch-decode-write pipeline
```

### Core Components

- **EventLog**: Raw RPC log records
- **UniversalDynColumns**: Dynamic columnar buffer for any projection
- **EventRegistry**: Configurable event decoding specifications
- **ShardAggregator**: Bounded-memory Parquet writer
- **LiveManifest**: Resumable operation tracking

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd define

# Install dependencies
pip install -e .
```

### Basic Usage

Refer to scripts in the `examples/` directory.

## 📋 Event Registries

The easiest way of building an event registry is to create it from an ABI file:

```python
from defind.abi_events import make_event_registry_from_abi
ABI = EXAMPLES_ROOT / "abi" / "aerodrome_clpool_abi.json"
registry = make_event_registry_from_abi(ABI)
```

DefInd comes with pre-built registries for common DeFi protocols:

### Concentrated Liquidity Pools
```python
from defind.decoding.registries import make_clpool_registry
registry = make_clpool_registry()
# Supports: Mint, Burn, Collect, CollectFees events
```

### Gauge Events
```python
from defind.decoding.registries import make_gauge_registry
registry = make_gauge_registry()
# Supports: Deposit, Withdraw, ClaimRewards events
```

### VFAT Events
```python
from defind.decoding.registries import make_vfat_registry
registry = make_vfat_registry()
# Supports: Deploy and other VFAT-specific events
```

### Custom Registries
```python
from defind.decoding.specs import EventSpec, TopicFieldSpec, DataFieldSpec

# Define custom event specification
custom_spec = EventSpec(
    topic0="0x...",
    name="CustomEvent",
    topic_fields=[
        TopicFieldSpec("user", 1, "address"),
        TopicFieldSpec("amount", 2, "uint256"),
    ],
    data_fields=[
        DataFieldSpec("timestamp", 0, "uint256"),
    ],
    projection={
        "user_address": ProjectionRefs.TopicRef(name="user"), # Output column "user_address" will be filled with "user" indexed event input
        "token_amount": ProjectionRefs.TopicRef(name="amount"),# Output column t"oken_amount" will be filled with "amount" indexed event input
        "event_time": ProjectionRefs.DataRef(name="timestamp"), # Output column "event_time" will be filled with "timestamp" unindexed event input
        "custom_field": ProjectionRefs.Constant(value="my_custom_value"), # Output column "custom_field" will be filled with constant "my_custom_value" for all "CustomEvent"
    }
)
```

## 🔧 Configuration

### Key Parameters

- **`rows_per_shard`**: Number of rows per Parquet file (default: 250,000)
- **`batch_decode_rows`**: Batch size for decoding (default: 50,000)
- **`concurrency`**: Max parallel RPC requests (default: 16)
- **`min_split_span`**: Minimum block range for splitting on failures

### Performance Tuning

```python
# High-throughput configuration
result = await fetch_decode(
    # ... other params
    rows_per_shard=500_000,     # Larger shards
    batch_decode_rows=100_000,  # Larger batches
    concurrency=32,             # More parallelism
    min_split_span=100,         # Aggressive splitting
)
```

## 📊 Output Format

DefInd produces Parquet files with a universal schema:

### Base Columns (always present)
- `block_number`: Block number (int64)
- `block_timestamp`: Block timestamp (int64)
- `tx_hash`: Transaction hash (string)
- `log_index`: Log index within transaction (int32)
- `contract`: Contract address (string)
- `event`: Event name (string)

### Dynamic Columns
Any key in your event registry's `projection` mapping becomes a column automatically, stored as strings for safety with large integers.

## 🛠️ Development

### Project Structure
```
src/defind/
├── __init__.py              # Public API exports
├── core/
│   ├── models.py           # Core data models
│   └── constants.py        # Event topic constants
├── clients/
│   └── rpc.py             # Ethereum RPC client
├── decoding/
│   ├── specs.py           # Event specifications
│   ├── decoder.py         # Generic event decoder
│   ├── registries.py      # Pre-built registries
│   └── utils.py           # Decoding utilities
├── storage/
│   ├── shards.py          # Universal shard writer
│   └── manifest.py        # Live manifest system
└── orchestration/
    ├── orchestrator.py    # Main streaming pipeline
    └── utils.py           # Block range utilities
```

### Dependencies
- **httpx**: Async HTTP client for RPC calls
- **pyarrow**: Columnar data processing and Parquet I/O
- **pandas**: Data manipulation and analysis
- **eth-abi**: Ethereum ABI encoding/decoding
- **eth-utils**: Ethereum utility functions
- **rich**: Beautiful terminal output
- **click**: CLI framework

## 📈 Performance

DefInd is optimized for high-throughput data extraction:

- **Concurrent Processing**: Configurable parallelism for RPC requests
- **Bounded Memory**: Streaming processing with configurable batch sizes
- **Efficient Storage**: Columnar Parquet format with compression
- **Smart Resumption**: Skip already-processed ranges automatically
- **Fast Filtering**: Early zero-word detection to skip empty events

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

Built for the DeFi community with ❤️ by [youssefGha98](mailto:youssef@nomiks.io)
