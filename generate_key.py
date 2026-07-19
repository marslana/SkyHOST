# generate_key.py
import os
key_bytes = os.urandom(32) # Generates 32 random bytes
with open("dev_e2ee_key.bin", "wb") as f: # Save as a binary file
    f.write(key_bytes)
print(f"Generated dev_e2ee_key.bin (32 random bytes, {len(key_bytes)} bytes long)")
