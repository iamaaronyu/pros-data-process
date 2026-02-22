# 测试用数据集

- `make_dataset.py`：生成小型图片数据集（最小合法 PNG），供端到端测试使用。
- 运行：`python tests/fixtures/make_dataset.py --out tests/fixtures/images -n 5`
- 或由 pytest fixture 在临时目录自动生成，无需预先创建。
