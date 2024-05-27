import matplotlib.pyplot as plt
import networkx as nx

# 创建有向图
G = nx.DiGraph()

# 添加节点和边
for index, row in path_chain_counts_pd.iterrows():
    paths = row['path_chain'].split(" -> ")
    for i in range(len(paths) - 1):
        if G.has_edge(paths[i], paths[i+1]):
            G[paths[i]][paths[i+1]]['weight'] += row['count']
        else:
            G.add_edge(paths[i], paths[i+1], weight=row['count'])

# 设置节点大小和边的宽度
node_size = [G.out_degree(node, weight='weight')*100 for node in G.nodes()]
edge_width = [d['weight']*0.1 for u, v, d in G.edges(data=True)]

# 绘制图表并保存为图片
plt.figure(figsize=(14, 10))
pos = nx.spring_layout(G, k=0.5)  # 使用spring布局
nx.draw(G, pos, with_labels=True, node_size=node_size, node_color="skyblue", font_size=10, font_weight="bold", width=edge_width)
edge_labels = {(u, v): d['weight'] for u, v, d in G.edges(data=True)}
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
plt.title("用户访问链路图")
plt.savefig('user_path_graph.png')
plt.close()
