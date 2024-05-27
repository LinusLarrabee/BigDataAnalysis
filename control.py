import subprocess

# 定义要执行的脚本文件列表
scripts = ["link2.py.py", "script2.py", "script3.py", "script4.py"]

# 遍历脚本列表并依次执行
for script in scripts:
    subprocess.run(["python", script])
