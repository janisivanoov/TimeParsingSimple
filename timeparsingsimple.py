from web3 import Web3, HTTPProvider
import eth_abi
import mysql.connector
from mysql.connector import errorcode

provider = HTTPProvider("https://bsc-dataseed1.binance.org/")
w3 = Web3(provider)

N_BLOCKS = 100

# Config for database
config = {
    'user': 'db_user_1',
    'password': 'db_user_password',
    'host': 'localhost',
    'database': 'timeparsingsimple',
    'raise_on_warnings': True
}

# Initializing MySQL connection
try:
  my_cn = mysql.connector.connect(**config)
except mysql.connector.Error as err:
    print(err)
    quit()
else:
  print ("Connected to the Database")

cursor =  my_cn.cursor() 

block_number = w3.eth.get_block_number()
print(f"block_number: {block_number}, requesting last {N_BLOCKS} blocks")

logs = w3.eth.get_logs(
    {
        "topics": [
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
        ],
        "fromBlock": block_number - N_BLOCKS,
    }
)

def getBlock():
    q_block = "SELECT Max(block_number) from timeparsingsimple.main"
    cursor.execute(q_block)
    for (block) in cursor:
        start_block = block
    if start_block == (None,):
        start_block = (0,)
    return start_block[0]

for log in logs:
    amount0_in, amount1_in, amount0_out, amount1_out = eth_abi.decode_abi(
        ("uint", "uint", "uint", "uint"), bytes.fromhex(log.data[2:])
    )
    insertion = "insert into main (amountIn, amountOut, block_number) values (%s, %s, %s)"
    amount0All = amount0_in - amount0_out;
    amount1All = amount1_in - amount1_out;
    cursor.execute(insertion, [amount0All, amount1All, block_number+1])
    print(amount0All, amount1All, block_number)
