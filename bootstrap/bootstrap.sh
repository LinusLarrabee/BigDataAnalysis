#!/bin/bash

# 更新系统
sudo yum update -y

# 安装Jupyter Notebook
sudo pip install jupyter

# 从S3下载requirements.txt文件
aws s3 cp s3://beta-tauc-data-analysis/initial-setup/requirements.txt /home/hadoop/requirements.txt

# 安装requirements.txt中的Python包
sudo pip install -r /home/hadoop/requirements.txt

# 配置Jupyter Notebook
jupyter notebook --generate-config

# 配置Jupyter Notebook以使用EMR的Spark
echo "c.NotebookApp.ip = '0.0.0.0'" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.open_browser = False" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.port = 8888" >> ~/.jupyter/jupyter_notebook_config.py

# Spark环境变量
echo "export SPARK_HOME=/usr/lib/spark" >> ~/.bashrc
echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.bashrc
echo "export PYSPARK_DRIVER_PYTHON=jupyter" >> ~/.bashrc
echo "export PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port=8888 --ip=0.0.0.0'" >> ~/.bashrc

# 启动Jupyter Notebook（可以根据需要配置成服务）
source ~/.bashrc
nohup pyspark &

# 下载配置文件（如有需要）
# aws s3 cp s3://beta-tauc-data-analysis/initial-setup/config-file /home/hadoop/config-file
# sudo mv /home/hadoop/config-file /etc/your-config-file
