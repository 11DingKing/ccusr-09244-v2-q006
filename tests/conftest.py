import os
import tempfile

# 在导入任何 app 模块之前，把默认数据库指向临时文件，
# 避免 main.py 导入时的建库逻辑在仓库根目录生成 robot_data.db。
_DEFAULT_DB_DIR = tempfile.mkdtemp(prefix="robot-data-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(_DEFAULT_DB_DIR, 'default.db')}"
