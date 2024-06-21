import json

# 原始 JSON 字符串
json_string = r'{"messageId":"89f722a4-b18f-43a7-bd99-5cebd781150f","totalSize":444,"size":444,"totalIndex":1,"index":1,"payload":"{\"filterKey\":\"data_collector\",\"message\":\"{\\\"dataCollectorDTO\\\":{\\\"tz\\\":8,\\\"lg\\\":\\\"zh-CN\\\",\\\"uvi\\\":\\\"b1902f4c3ab5bf73\\\",\\\"sr\\\":\\\"tauc\\\",\\\"srp\\\":{\\\"apv\\\":null,\\\"plv\\\":\\\"windows 10-Edge 126.0.0.0\\\",\\\"scr\\\":\\\"1920*1080+1\\\",\\\"scl\\\":\\\"1872*966\\\"},\\\"el\\\":[{\\\"eid\\\":\\\"ReportCenter\\\",\\\"ep\\\":{\\\"be\\\":\\\"Ins\\\"},\\\"ct\\\":1718780443335,\\\"path\\\":null,\\\"usi\\\":null,\\\"pvi\\\":null}],\\\"ex\\\":null,\\\"accountId\\\":\\\"\\\"},\\\"id\\\":\\\"1718780445\\\"}\",\"timeStamp\":1718780445988}","compressionType":null,"compressionLevel":-1,"enableSlice":false}'

# 解析最外层 JSON
parsed_json = json.loads(json_string)

# 获取并解析嵌套的 JSON
payload_str = parsed_json['payload']
nested_json = json.loads(payload_str)

# 获取并解析更深层次的嵌套 JSON
message_str = nested_json['message']
deep_nested_json = json.loads(message_str)

# 提取 el 列表中的字段
el_list = deep_nested_json['dataCollectorDTO']['el']

# 筛选出 eid = "pageView" 的条目并提取所需字段
result = [{'eid': el['eid'], 'ep': el['ep'], 'ct': el['ct']} for el in el_list if el['eid'] == 'pageView']

# 输出结果
print(result)
