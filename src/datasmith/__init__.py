import os

with open("tokens.env", encoding="utf-8") as f:
    lines = f.readlines()
    tokens = {line.split("=")[0].strip(): line.split("=")[1].strip() for line in lines if "=" in line}
os.environ.update(tokens)
