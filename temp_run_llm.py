import requests
resp = requests.post("http://127.0.0.1:8000/api/v1/phases/phase14-train-models", data={"target_column":"STATUS_Return","primary_metric":"recall","domain":"logistics"})
print("phase14", resp.status_code)
print(resp.text.encode('unicode_escape'))
resp = requests.post("http://127.0.0.1:8000/api/v1/llm-analysis/run-analysis")
print("phase14.5", resp.status_code)
print(resp.text.encode('unicode_escape'))
