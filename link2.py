import plotly.graph_objects as go
import pandas as pd

# 读取CSV文件
file_path = 'path_chain_counts.csv'
data = pd.read_csv(file_path)

# 解析数据源
paths = []
weights = []
for index, row in data.iterrows():
    path = row['path_chain']
    weight = int(row['count'])
    pages = path.split(' -> ')

    # 生成带有顺序的标签
    for i in range(len(pages) - 1):
        source_label = f"{pages[i]} ({i+1})"
        target_label = f"{pages[i+1]} ({i+2})"
        paths.append((source_label, target_label))
        weights.append(weight)

# 创建标签列表和索引映射
labels = list(set([item for sublist in paths for item in sublist]))
label_index = {label: i for i, label in enumerate(labels)}

# 创建source和target列表
source = [label_index[path[0]] for path in paths]
target = [label_index[path[1]] for path in paths]

# 创建Sankey图
fig = go.Figure(go.Sankey(
    node=dict(
        pad=15,
        thickness=20,
        line=dict(color="black", width=0.5),
        label=labels,
        color="blue"
    ),
    link=dict(
        source=source,
        target=target,
        value=weights
    )
))

# 更新布局
fig.update_layout(
    title_text="User Navigation Paths",
    font_size=10
)

fig.write_image("/mnt/data/user_navigation_paths.png")

fig.show()
