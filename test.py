import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx

# 读取数据
data = pd.read_csv('/Users/sunhao/Downloads/scala_demo/user_visit_chains.csv/part-00000-10e4418d-c8e2-48ce-a750-116d2ad3643d-c000.csv')

# 准备节点和边数据
nodes = set()
edges = []
for _, row in data.iterrows():
    for i in range(len(row) - 1):
        if pd.isnull(row[i]) or pd.isnull(row[i + 1]):
            break
        source = row[i]
        target = row[i + 1]
        nodes.add(source)
        nodes.add(target)
        edges.append((source, target))

# 创建有向图
G = nx.DiGraph()
G.add_nodes_from(nodes)
G.add_edges_from(edges)

# 计算布局
pos = nx.spring_layout(G)

# 绘制节点
nx.draw_networkx_nodes(G, pos, node_size=7000, node_color='skyblue')

# 绘制边
nx.draw_networkx_edges(G, pos, arrowstyle='->', arrowsize=20, edge_color='black')

# 绘制标签
labels = nx.draw_networkx_labels(G, pos, font_size=12, font_color='black')

plt.title('User Visit Flow')
plt.axis('off')
# 保存图表到本地文件
plt.savefig("page_transitions.png")

# 显示图表
plt.show()
