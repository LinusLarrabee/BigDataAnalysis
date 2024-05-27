import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx

# 读取数据
data = pd.read_csv('/Users/sunhao/Downloads/scala_demo/page_transitions.csv/part-00000-cff5fdf5-04ef-42a2-86cd-ddb6ba87e465-c000.csv')

# 创建有向图
G = nx.DiGraph()

# 添加边和权重
for _, row in data.iterrows():
    G.add_edge(row['page'], row['next_page'], weight=row['count'])

# 计算布局
pos = nx.spring_layout(G)

# 绘制节点
nx.draw_networkx_nodes(G, pos, node_size=7000, node_color='skyblue')

# 绘制边
edges = nx.draw_networkx_edges(G, pos, arrowstyle='->', arrowsize=20, edge_color='black')
for edge in edges:
    edge.set_connectionstyle('arc3,rad=0.2')

# 绘制标签
labels = nx.draw_networkx_labels(G, pos, font_size=12, font_color='black')

# 绘制边的权重
edge_labels = nx.get_edge_attributes(G, 'weight')
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)

plt.title('Page Transitions Flow')
plt.axis('off')
plt.show()
