# index_pump_data.py
import json
import asyncio
from time import sleep
from datetime import timezone, datetime
from typing import List, Dict, Any

import duckdb
import requests
from base58 import b58encode, b58decode
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.transaction import VersionedTransaction
from websockets import connect

# --- CONFIGURATION ---
# !! 1. REPLACE THIS WITH YOUR WALLET'S PUBLIC KEY (as a string) !!
PUBKEY = "YOUR_PUBKEY_HERE" 
# e.g., "7gFPc...dG3a"
# !! 2. Make sure you have a file named `wallets/YOUR_PUBKEY_HERE.json` !!
#    This is your wallet's private key, e.g., from Phantom (Export Private Key)
#    NEVER share this file or commit it to git!
HTTP_API = "https://pumpstream.io/api/v1"
WS_API = "wss://pumpstream.io/api/v1"
DEPOSIT_AMOUNT = 0.001  # 0.001 SOL for 1 hour of stream time
# ---------------------

def load_keypair_from_file(pubkey_str: str) -> Keypair:
    """Loads a wallet's keypair from its JSON file."""
    # Assumes a folder named 'wallets' in the same directory
    try:
        with open(f"wallets/{pubkey_str}.json", "r") as f:
            keypair_data = json.load(f)
        return Keypair.from_bytes(bytes(keypair_data))
    except FileNotFoundError:
        print(f"Error: Keypair file not found at 'wallets/{pubkey_str}.json'")
        print("Please create the 'wallets' folder and export your keypair JSON to it.")
        exit(1)

def get_auth_signature(keypair: Keypair, message: str) -> str:
    """Signs a string message and returns the base58 encoded signature."""
    signature_bytes = keypair.sign_message(message.encode('utf-8')).__bytes__()
    return b58encode(signature_bytes).decode('utf-8')


# --- Load our wallet and signature ---
# We'll need these for all API requests
try:
    balance_wallet = load_keypair_from_file(PUBKEY)
    auth_signature = get_auth_signature(balance_wallet, PUBKEY)
except Exception as e:
    print(f"Failed to load wallet or create signature: {e}")
    exit(1)
# -------------------------------------

def check_and_top_up_balance():
    """Check balance and top it up if needed."""
    print("🔐 Checking account balance...")
    balance_url = f"{HTTP_API}/balance?pubkey={PUBKEY}&signed_message={auth_signature}"
    
    try:
        response = requests.get(balance_url).json()
        sleep(1)  # Avoid rate limits
        if "data" not in response or "expires_in_hours" not in response.get("data", {}):
            print(f"⚠️  Could not parse balance. Response: {response}")
            # Assume no balance, try to deposit
            expires_in_hours = 0
        else:
             expires_in_hours = response["data"]["expires_in_hours"]
        if expires_in_hours < 1:
            print(f"⚠️  Balance is low ({expires_in_hours:.2f} hours). Initiating a deposit...")
            
            # 1. Generate the transaction
            gen_tx_url = f"{HTTP_API}/deposit/generate-transaction?pubkey={PUBKEY}&amount={DEPOSIT_AMOUNT}"
            gen_response = requests.get(gen_tx_url).json()
            unsigned_tx_b58 = gen_response["data"]["unsigned_message"]
            
            # 2. Reconstruct and sign
            message_v0 = MessageV0.from_bytes(b58decode(unsigned_tx_b58))
            signed_tx = VersionedTransaction(message_v0, [balance_wallet])
            signed_tx_b58 = b58encode(signed_tx.__bytes__()).decode('utf-8')
            
            sleep(1)  # Avoid rate limits
            
            # 3. Submit
            submit_tx_url = f"{HTTP_API}/deposit/submit-transaction?pubkey={PUBKEY}&signed_transaction={signed_tx_b58}"
            deposit_response = requests.get(submit_tx_url).json()
            
            if deposit_response.get("success"):
                print(f"✅ Deposit successful! New balance: {deposit_response['data']}")
            else:
                print(f"🚨 Deposit FAILED: {deposit_response}")
                print("Please check your wallet has > 0.001 SOL and try again.")
                exit(1)
                
            sleep(1)  # Avoid rate limits
        else:
            print(f"✅ Balance is sufficient: {response['data']}")
            
    except Exception as e:
        print(f"⚠️  Could not check/top-up balance: {e}")
        print("Continuing anyway, but the stream might fail...")


class DuckDBDatabase:
    def __init__(self, db_path: str = "pumpstream_transactions.duckdb"):
        self.db_path = db_path
        self.conn = None
    
    async def initialize(self):
        # DuckDB is synchronous, so we run its setup
        # in an asyncio-friendly way (in a separate thread pool)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._initialize_sync)
        print(f"Database initialized: {self.db_path}")
    
    def _initialize_sync(self):
        """This runs synchronously in the executor."""
        self.conn = duckdb.connect(self.db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                signature VARCHAR PRIMARY KEY,
                signer_address VARCHAR,
                mint_address VARCHAR,
                side VARCHAR,
                sol_amount DOUBLE,
                token_amount DOUBLE,
                price DOUBLE,
                bonding_curve_pc DOUBLE,
                is_mint_creation BOOLEAN,
                block BIGINT,
                block_hash VARCHAR,
                block_time TIMESTAMP,
                timestamp TIMESTAMP,
                compute_budget BIGINT,
                priority_fee DOUBLE,
                priority_fee_rate BIGINT,
                fingerprint VARCHAR,
                accounts JSON,
                received_at TIMESTAMP
            )
        """)
        # Indexes make querying by these columns *much* faster
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_signer ON transactions(signer_address)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_mint ON transactions(mint_address)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON transactions(timestamp)")
        
        # A handy view for just looking at new tokens
        self.conn.execute("""
            CREATE OR REPLACE VIEW creations AS
            SELECT * FROM transactions WHERE is_mint_creation = true
        """)
    
    async def insert_transactions(self, transactions: List[Dict[str, Any]]):
        if not transactions:
            return
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._insert_sync, transactions)
        print(f"Inserted {len(transactions)} transactions")
    
    def _insert_sync(self, transactions: List[Dict[str, Any]]):
        """Batch insert data. This is much faster than one-by-one."""
        data = [
            (
                tx.get("signature"), tx.get("signer_address"), tx.get("mint_address"),
                tx.get("side"), tx.get("sol_amount"), tx.get("token_amount"),
                tx.get("price"), tx.get("bonding_curve_pc"), tx.get("is_mint_creation", False),
                tx.get("block"), tx.get("block_hash"), tx.get("block_time"),
                tx.get("timestamp"), tx.get("compute_budget"), tx.get("priority_fee"),
                tx.get("priority_fee_rate"), tx.get("fingerprint"),
                json.dumps(tx.get("accounts")) if tx.get("accounts") else None,
                datetime.now(timezone.utc).isoformat()
            )
            for tx in transactions
        ]
        
        # 'executemany' is the key to speed here.
        # ON CONFLICT (signature) DO NOTHING makes it idempotent.
        # If we get the same transaction twice, it just ignores it.
        self.conn.executemany("""
            INSERT INTO transactions (
                signature, signer_address, mint_address, side, sol_amount, token_amount,
                price, bonding_curve_pc, is_mint_creation, block, block_hash, block_time,
                timestamp, compute_budget, priority_fee, priority_fee_rate, fingerprint,
                accounts, received_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (signature) DO NOTHING
        """, data)
    
    async def close(self):
        if self.conn:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.conn.close)


async def receive_stream(queue: asyncio.Queue):
    """Stream transactions from pumpstream and queue them for batch insertion"""
    ws_auth_url = f"{WS_API}/stream?pubkey={PUBKEY}&signed_message={auth_signature}"
    
    print(f"📡 Connecting to WebSocket stream: {ws_auth_url}")
    async with connect(ws_auth_url) as websocket:
        print("✅ Connected to pumpstream! Waiting for data...")
        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=20.0)
                data = json.loads(message)
                for tx in data:
                    queue.put_nowait(tx) # Put it in the queue and don't block
            except asyncio.TimeoutError:
                print("WebSocket timeout. Reconnecting...")
                # The 'async with' will handle reconnection on the next loop
                break 
            except Exception as e:
                print(f"Error in stream: {e}. Reconnecting...")
                break # Break to force reconnection


async def main():
    # 1. Check and top up balance
    check_and_top_up_balance()
    
    # 2. Initialize database
    db = DuckDBDatabase()
    await db.initialize()
    
    # 3. Create our shared queue
    queue = asyncio.Queue()
    
    # 4. Run the producer (stream) and consumer (db worker)
    try:
        await asyncio.gather(
            receive_stream(queue),
            batch_insert_worker(queue, db)
        )
    except Exception as e:
        print(f"Main loop error: {e}")
    finally:
        print("Closing database connection...")
        await db.close()


async def batch_insert_worker(queue: asyncio.Queue, db: DuckDBDatabase):
    """Batch insert transactions every 5 seconds or every 100 txs."""
    batch = []
    last_insert_time = asyncio.get_event_loop().time()
    
    while True:
        try:
            # Wait for an item, but with a timeout
            timeout = max(0.1, 5.0 - (asyncio.get_event_loop().time() - last_insert_time))
            tx = await asyncio.wait_for(queue.get(), timeout=timeout)
            batch.append(tx)
            
            # Insert if batch is big or 5 seconds have passed
            time_since_last_insert = asyncio.get_event_loop().time() - last_insert_time
            if len(batch) >= 100 or time_since_last_insert >= 5.0:
                await db.insert_transactions(batch)
                batch = []
                last_insert_time = asyncio.get_event_loop().time()
                
        except asyncio.TimeoutError:
            # Timeout happened, so 5 seconds have passed
            # Insert whatever we have in the batch
            if batch:
                await db.insert_transactions(batch)
                batch = []
                last_insert_time = asyncio.get_event_loop().time()
        except Exception as e:
            print(f"Error in batch inserter: {e}")


if __name__ == "__main__":
    print("Starting PumpStream indexer...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nManually stopped. Exiting.")
