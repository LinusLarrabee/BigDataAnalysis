import matplotlib.pyplot as plt
import networkx as nx
from collections import defaultdict

# 定义用户访问数据
data = {
    "uvi": "user495",
    "el": [
        {"path": "/faq", "ct": "2024-05-27T11:40:36.263274"},
        {"path": "/services", "ct": "2024-05-27T11:44:36.263274"},
        {"path": "/contact", "ct": "2024-05-27T11:48:36.263274"},
        {"path": "/services", "ct": "2024-05-27T11:55:36.263274"},
        {"path": "/faq", "ct": "2024-05-27T11:44:36.263274"},
        {"path": "/about", "ct": "2024-05-27T12:00:36.263274"}
    ]
}

# 构建树状图
G = nx.DiGraph()
user_paths = defaultdict(list)

# 根据时间戳排序访问路径
sorted_events = sorted(data["el"], key=lambda x: x["ct"])
for i in range(len(sorted_events) - 1):
    source = sorted_events[i]["path"]
    target = sorted_events[i + 1]["path"]
    G.add_edge(source, target)

# 生成布局
pos = nx.spring_layout(G, k=1.5, iterations=50)

# 绘制节点
nx.draw_networkx_nodes(G, pos, node_size=7000, node_color='lightblue')

# 绘制边
nx.draw_networkx_edges(G, pos, edgelist=G.edges(), edge_color='grey')

# 绘制标签
nx.draw_networkx_labels(G, pos, font_size=10)

# 显示图表
plt.title("用户访问路径树状图")
plt.show()
