import os

os.chdir(os.getcwd())  
for script in ["create_cluster.py", "create_tables.py", "etl.py"]:
    with open(script) as f:
        contents = f.read()
    exec(contents)