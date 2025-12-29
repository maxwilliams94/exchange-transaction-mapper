# Transaction Mapping Converter

A Python-based CSV converter for normalizing cryptocurrency transaction exports from multiple exchanges into a unified format. The converter is fully configuration-driven via YAML, making it easy to add new exchanges or modify mapping rules.

## Overview

The converter processes CSV exports from various cryptocurrency exchanges (Coinbase, Firi, NBX, Kraken) and transforms them into a standardized format suitable for tax reporting, portfolio tracking, and analysis.

### Key Features

- **YAML-Driven Configuration**: All mapping rules, column expectations, and transformations are defined in `config.yaml`
- **Multi-Exchange Support**: Built-in support for Coinbase, Firi, NBX, and Kraken
- **Transaction Type Filtering**: Automatically filter out unwanted transaction types (e.g., deposits/withdrawals)
- **Sequential ID Assignment**: Generate consistent, sequential IDs across outputs
- **Flexible Mapping Modes**: Support for row-by-row and file-level handlers
- **Expression Engine**: Dynamic field mapping with helper functions for timestamps, decimals, and market formatting

## Installation

### Prerequisites

- Python 3.8 or higher
- Virtual environment (recommended)

### Setup

1. Clone or navigate to the repository:
```bash
cd /path/to/transaction-mapping
```

2. Create and activate a virtual environment:
```bash
# macOS/Linux
python3 -m venv .venv
source .venv/bin/activate

# Windows
python -m venv .venv
.\.venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Command

```bash
python -m src.converter -i <input_dir> -o <output_dir> -c config.yaml
```

### Command-Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Input directory containing CSV files (required) | - |
| `--output` | `-o` | Output directory for mapped CSV files (required) | - |
| `--config` | `-c` | Path to YAML configuration file | `config.yaml` |
| `--verbose` | `-v` | Enable verbose output | `false` |
| `--dry-run` | - | Parse and convert without writing output files | `false` |

### Examples

**Process all CSV files from the input directory:**
```bash
python -m src.converter -i files/input -o files/output/mapped -c config.yaml
```

**Dry-run to preview conversion without creating files:**
```bash
python -m src.converter -i files/input -o files/output/mapped -c config.yaml --dry-run
```

**Process a specific exchange:**
```bash
python -m src.converter -i files/input/kraken -o files/output/mapped -c config.yaml
```

**Verbose output for debugging:**
```bash
python -m src.converter -i files/input -o files/output/mapped -c config.yaml --verbose
```

## Directory Structure

```
transaction-mapping/
├── config.yaml              # Main configuration file
├── requirements.txt         # Python dependencies
├── files/
│   ├── input/              # Place your CSV exports here
│   │   ├── coinbase/
│   │   ├── firi/
│   │   ├── kraken/
│   │   └── nbx/
│   └── output/
│       └── mapped/         # Converted CSV files appear here
└── src/
    └── converter/
        ├── __main__.py     # CLI entry point
        ├── mapping_engine.py
        ├── constants.py
        ├── utils.py
        └── mappers/        # Exchange-specific handlers
            ├── coinbase.py
            ├── firi.py
            ├── kraken.py
            └── nbx.py
```

## Input File Organization

Place your CSV exports in the appropriate subdirectory under `files/input/`:

```
files/input/
├── coinbase/
│   ├── coinbase_transactions_2024.csv
│   └── coinbase_rewards_2024.csv
├── firi/
│   ├── firi_2024_transactions.csv
│   └── firi_2025_transactions.csv
├── kraken/
│   └── kraken_ledger_2025.csv
└── nbx/
    ├── NBX_annual_report_2024.csv
    └── NBX_annual_report_2025.csv
```

The converter automatically detects the exchange based on file patterns defined in `config.yaml`.

## Output Format

All mapped files follow a standardized CSV format with these columns:

| Column | Description |
|--------|-------------|
| `Id` | Unique transaction identifier (auto-generated or exchange-provided) |
| `ExchangeId` | Original transaction ID from the exchange |
| `Timestamp` | ISO 8601 timestamp |
| `Status` | Transaction status (usually `COMPLETED`) |
| `Market` | Trading pair in `BASE-QUOTE` format (e.g., `BTC-EUR`, `ETH-NOK`) |
| `Exchange` | Exchange name (`COINBASE`, `FIRI`, `KRAKEN`, `NBX`) |
| `Side` | Transaction side: `BUY`, `SELL`, `DEPOSIT`, `WITHDRAW` |
| `TransactionType` | Type: `TRADE`, `AIRDROP`, `STAKING_REWARD`, `DEPOSIT`, `WITHDRAWAL` |
| `FilledQuantity` | Amount of base currency |
| `FilledQuote` | Amount of quote currency (for trades) |
| `FilledPrice` | Price per unit (quote/base) |
| `Fee` | Transaction fee amount |
| `FeeCurrency` | Currency of the fee |

### Example Output

```csv
Id,ExchangeId,Timestamp,Status,Market,Exchange,Side,TransactionType,FilledQuantity,FilledQuote,FilledPrice,Fee,FeeCurrency
kraken-000001,TXID123,2025-01-15T10:30:00+00:00,COMPLETED,BTC-EUR,KRAKEN,BUY,TRADE,0.5,15000,30000,10,EUR
kraken-000002,TXID124,2025-01-20T14:45:00+00:00,COMPLETED,ETH,KRAKEN,BUY,STAKING_REWARD,2.5,,,,
```

## Configuration

The `config.yaml` file controls all mapping behavior. Each exchange has its own section under `sources`:

```yaml
sources:
  kraken:
    files:
      - pattern: "*ledger*.csv"
        mode: file
        handler: kraken_ledger
        expected_columns:
          - txid
          - time
          - type
        ignore_transaction_types:
          - DEPOSIT
          - WITHDRAWAL
        id_sequence_prefix: kraken
        id_sequence_padding: 6
```

### Key Configuration Options

- **pattern**: File name pattern to match (glob-style)
- **mode**: `row` (row-by-row mapping) or `file` (custom handler) or `skip`
- **handler**: Custom Python function for file-level processing
- **expected_columns**: List of required column headers
- **ignore_transaction_types**: Filter out specific transaction types from output
- **id_sequence_prefix**: Prefix for auto-generated IDs
- **id_sequence_padding**: Zero-padding width for sequential IDs

## Supported Exchanges


### Coinbase

- **File Pattern**: `*.csv` (standard transaction exports)
- **Supported Exports**: Standard transaction exports from Coinbase Pro/Advanced
- **Transaction Types**: Buy, Sell, Rewards, Airdrops, Deposits, Withdrawals

#### Coinbase Transaction Type Mapping

| Exchange Transaction Type | Mapped Type | Notes |
|--------------------------|-------------|-------|
| `Buy`, `Advanced Trade Buy` | `TRADE` | Buy transactions |
| `Sell`, `Advanced Trade Sell` | `TRADE` | Sell transactions |
| `Reward Income`, `Staking Income` | `STAKING_REWARD` | Staking and reward income |
| `Receive` | `STAKING_REWARD` | Airdrops and receives (treated as rewards) |
| `Airdrop` | `AIRDROP` | Airdrop transactions |
| `Deposit` | `DEPOSIT` | User deposits |
| `Withdrawal`, `Send` | `WITHDRAW` | User withdrawals |
| `Exchange Deposit` | `INTERNAL_TRANSFER` | Internal exchange transfers |
| `Exchange Withdrawal` | `INTERNAL_TRANSFER` | Internal exchange transfers |
| `Retail Staking Transfer`, `Retail Unstaking Transfer` | `INTERNAL_TRANSFER` | Staking transfers |
| `Pro Withdrawal` | **EXCLUDED** | Pro account withdrawals (phantom transactions) |
| `Pro Deposit` | **EXCLUDED** | Pro account deposits (phantom transactions) |

**Key Processing Rules:**
- **Advanced Trades**: Parsed from notes field to extract actual quote currency and amounts
- **Crypto/Crypto Trades**: Quotes in NOK (or native currency) when price currency is not fiat
- **Fee Handling**: Extracted from "Fees and/or Spread" column; currency derived from price currency
- **Pro Deposits/Withdrawals**: Excluded from processing as they create phantom buy/sell transactions

### Firi

- **File Pattern**: `*transactions*.csv`
- **Supported Exports**: Transaction history including staking rewards
- **Transaction Types**: Trades, staking rewards, deposits, withdrawals
- **Notes**: Uses file handler to group related transactions; deposits/withdrawals filtered by default

### NBX

- **File Pattern**: `NBX_annual_report_*.csv`
- **Supported Exports**: Annual report exports (semicolon-delimited)
- **Transaction Types**: Trades, deposits, withdrawals
- **Notes**: Automatically detects buy/sell side based on In/Out currencies

### Kraken

- **File Pattern**: `*ledger*.csv`
- **Supported Exports**: Ledger exports
- **Transaction Types**: Trades, staking rewards, airdrops
- **Notes**: Groups related ledger entries by `refid`; automatically classifies rewards vs trades

## Transaction Type Filtering

By default, the converter filters out `DEPOSIT` and `WITHDRAWAL` transaction types for exchanges where this is configured. You can customize this in `config.yaml`:

```yaml
This produces IDs like: `kraken-000001`, `kraken-000002`, etc.
### Firi

- **File Pattern**: `*_transactions*.csv`
- **Supported Exports**: Transaction history including staking rewards, trades, deposits, withdrawals
- **Processing Mode**: File-level handler (groups and filters transactions)

#### Firi Transaction Type Mapping

| Exchange Action | Mapped Type | Side | Notes |
|-----------------|------------|------|-------|
| `Buy` | `TRADE` | `BUY` | Trade purchases |
| `Sell` | `TRADE` | `SELL` | Trade sales |
| `Staking Reward` | `STAKING_REWARD` | `BUY` | Staking rewards |
| `Deposit` | `DEPOSIT` | — | Filtered by default |
| `Withdrawal` | `WITHDRAW` | — | Filtered by default |

**Key Processing Rules:**
- File-level processing that groups related transactions by Match ID
- Default filtering removes DEPOSIT and WITHDRAW transactions (can be configured)
- Staking rewards mapped as BUY transactions for tax reporting
- Uses sequential ID generation with `firi` prefix

### NBX

- **File Pattern**: `NBX_annual_report_*.csv`
- **Supported Exports**: Annual report exports (semicolon-delimited format)
- **Processing Mode**: Row-by-row mapping

#### NBX Transaction Type Mapping

| Exchange Type | Mapped Type | Detection | Notes |
|-------------|------------|-----------|-------|
| `Trade` | `TRADE` | In/Out currencies differ | Automatic buy/sell detection |
| `Deposit` | `DEPOSIT` | Type = Deposit | User deposits |
| `Withdrawal` | `WITHDRAW` | Type = Withdrawal | User withdrawals |

**Key Processing Rules:**
- **Trade Detection**: Classified when In and Out currencies differ
- **Side Detection**: Determined by which currency is the primary base/quote pair
- **Fee Handling**: Extracted separately; currency provided in Fee-Currency column
- **No Filtering**: By default, all transaction types are included

### Kraken

- **File Pattern**: `*ledger*.csv`
- **Supported Exports**: Ledger exports from Kraken
- **Processing Mode**: File-level handler (groups by refid)

#### Kraken Transaction Type Mapping

| Ledger Type | Mapped Type | Notes |
|------------|------------|-------|
| `trade` | `TRADE` | Matched buy/sell ledger entries |
| `reward` | `STAKING_REWARD` | Staking and validation rewards |
| `airdrop` | `AIRDROP` | Airdrop distributions |
| `deposit` | `DEPOSIT` | User deposits |
| `withdrawal` | `WITHDRAW` | User withdrawals |

**Key Processing Rules:**
- **Refid Grouping**: Related ledger entries grouped by `refid` to reconstruct full trades
- **Trade Reconstruction**: Buy + Sell ledger entries for same trade combined with pricing
- **Reward Classification**: Automatically classifies staking rewards vs airdrop rewards
- **Fee Handling**: Extracted from separate fee ledger entries
- **Default Filtering**: DEPOSIT and WITHDRAWAL typically filtered in config

## Troubleshooting

### Missing Columns Error

If you see an error like:
```
## Mapping Rules Summary
Error: Missing expected columns in file.csv: ['expected_col']
### Standardized Transaction Types
```
The converter normalizes all exchange-specific transaction types into a standardized set:

| Standard Type | Purpose | Tax Implications | Notes |
|--------------|---------|-----------------|-------|
| `TRADE` | Buy/Sell transactions | Capital gains/losses | Base currency cost and proceeds |
| `STAKING_REWARD` | Staking/validation rewards | Taxable income | Treated as BUY at market value |
| `AIRDROP` | Free token distributions | Taxable income | No cost basis, only proceeds |
| `DEPOSIT` | Inbound transfers (fiat/crypto) | No tax impact | Typically filtered out |
| `WITHDRAW` | Outbound transfers (fiat/crypto) | No tax impact | Typically filtered out |
| `INTERNAL_TRANSFER` | Exchange-internal transfers | No tax impact | Excluded from processing |

### Side Determination Rules
**Solution**: Verify that your CSV export matches the expected format for that exchange, or update `expected_columns` in `config.yaml`.
The converter determines transaction sides (`BUY`/`SELL`/`DEPOSIT`/`WITHDRAW`) using these rules:

**For TRADE transactions:**
- If quantity is positive → `BUY`
- If quantity is negative → `SELL`
- Explicit side indicators in exchange data override quantity-based detection

**For DEPOSIT/WITHDRAW:**
- Derived directly from transaction type
- Usually omitted in output if transaction is filtered

**For REWARD/AIRDROP:**
- Treated as `BUY` (buying at market value on receipt date)
- Quantity is the amount received

### Price and Quantity Normalization
### File Not Processed
- **Quantities**: Stored as absolute values; side determines direction
- **Prices**: Calculated as `|total| / |quantity|` when not explicitly provided
- **Fee Currency**: Derived from price currency or transaction currency
- **Fee Amount**: Always stored as positive value

### Exchange-Specific Processing Notes
If a file is skipped, check:
**Coinbase Advanced Trades:**
- Notes field parsed to extract actual base/quote pair (e.g., ETH-USDC)
- Price currency may be NOK/fiat but actual trade is in different currency
- Pro account transactions (Pro Deposit/Pro Withdrawal) explicitly excluded to prevent phantom transactions

**Firi File-Level Processing:**
- Related transactions grouped by Match ID before mapping
- Allows aggregation of multi-leg transactions
- Deposits/withdrawals filtered by default

**NBX Trade Detection:**
- Trade identified when In and Out currencies differ
- Side (BUY/SELL) determined by currency pair and amount signs
- Semicolon-delimited format handled automatically

**Kraken Ledger Grouping:**
- Multiple ledger entries for single trade grouped by refid
- Reconstructs complete trade from buy and sell entries
- Handles fees as separate ledger entries

## Transaction Type Filtering
1. The file pattern in `config.yaml` matches your filename
2. The file is in the correct subdirectory under `files/input/`
3. The file isn't marked with `mode: skip` in the configuration

### Wrong Transaction Types

If transactions are classified incorrectly:
1. Check the exchange-specific mapper in `src/converter/mappers/`
2. Verify the logic in the `TransactionType` mapping expression
3. For Kraken, ensure ledger entries have correct `type` values (reward, trade, etc.)

### Empty Output Files

If mapped files contain no rows:
1. Check if transaction type filtering is too aggressive
2. Verify the CSV has data rows (not just headers)
3. Run with `--verbose` to see detailed processing logs

## Development

### Adding a New Exchange

1. Create a new mapper file: `src/converter/mappers/yourexchange.py`
2. Implement either:
   - Row-level mapping functions, or
   - File-level handler function (signature: `(file_path, rows, context) -> List[Dict]`)
3. Add configuration in `config.yaml` under `sources.yourexchange`
4. Register the handler in `src/converter/__main__.py` if using file mode

### Testing

Run the converter in dry-run mode to validate changes:
```bash
python -m src.converter -i files/test/input -o files/test/output --dry-run --verbose
```

Compile Python modules to check for syntax errors:
```bash
python -m compileall src
```

## License

See the main repository LICENSE file.

## Contributing

Contributions are welcome! Please ensure:
- New exchange mappers follow existing patterns
- Configuration changes are documented
- Code passes `python -m compileall src` without errors

## Support

For issues or questions, please refer to the repository issue tracker or documentation.
